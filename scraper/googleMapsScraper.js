const puppeteer = require("puppeteer");
const axios = require("axios");
const cheerio = require("cheerio");

const EMAIL_RE = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;

// Extract emails from a website's HTML (mailto links + body text)
async function extractEmails(url) {
  if (!url) return [];
  try {
    const { data } = await axios.get(url, {
      timeout: 8000,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
          "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      },
      maxRedirects: 3,
    });
    const $ = cheerio.load(data);

    const emails = new Set();

    // mailto links
    $('a[href^="mailto:"]').each((_, el) => {
      const href = $(el).attr("href") || "";
      const email = href.replace("mailto:", "").split("?")[0].trim().toLowerCase();
      if (email.match(EMAIL_RE)) emails.add(email);
    });

    // Emails in page text
    const bodyText = $("body").text();
    const found = bodyText.match(EMAIL_RE) || [];
    for (const e of found) {
      const clean = e.toLowerCase();
      // Skip image/file extensions that look like emails
      if (!/\.(png|jpg|jpeg|gif|svg|webp|css|js)$/i.test(clean)) {
        emails.add(clean);
      }
    }

    return [...emails];
  } catch {
    return [];
  }
}

// Scrape Google Maps: extract business name + website link, then emails from each site
async function scrapeGoogleMapsWithEmails(niche, location, opts = {}) {
  const { maxResults = 40, timeoutMs = 30000 } = opts;
  const query = `${niche} ${location}`;
  const url = `https://www.google.com/maps/search/${encodeURIComponent(query)}`;

  console.log(`  [GMaps Scraper] query: "${query}"`);

  let browser;
  try {
    browser = await puppeteer.launch({
      headless: "new",
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-extensions",
        "--single-process",
        "--no-zygote",
      ],
    });

    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 900 });
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    );

    await page.goto(url, { waitUntil: "networkidle2", timeout: timeoutMs });

    // Wait for results feed
    const FEED = 'div[role="feed"]';
    try {
      await page.waitForSelector(FEED, { timeout: 10000 });
    } catch {
      console.log("  [GMaps Scraper] No results feed found");
      return [];
    }

    // Scroll to load results
    let prev = 0, stale = 0;
    for (let i = 0; i < 15; i++) {
      await page.evaluate((s) => {
        const f = document.querySelector(s);
        if (f) f.scrollTop = f.scrollHeight;
      }, FEED);
      await page.waitForTimeout(1500);

      const count = await page.evaluate((s) => {
        const f = document.querySelector(s);
        return f ? f.children.length : 0;
      }, FEED);

      if (count >= maxResults) break;
      if (count === prev) { stale++; if (stale >= 3) break; }
      else stale = 0;
      prev = count;
    }

    // Extract name + link from each business card
    const raw = await page.evaluate(() => {
      const out = [];
      const cards = document.querySelectorAll("a.hfpxzc");
      for (const card of cards) {
        const name = card.getAttribute("aria-label") || "";
        const link = card.getAttribute("href") || "";
        if (name) out.push({ name: name.trim(), link });
      }
      return out;
    });

    console.log(`  [GMaps Scraper] found ${raw.length} businesses, clicking for websites...`);

    // Click each card to get the website URL from the detail panel
    const businesses = [];
    for (let i = 0; i < Math.min(raw.length, maxResults); i++) {
      try {
        const cards = await page.$$("a.hfpxzc");
        if (!cards[i]) continue;

        await cards[i].click();
        await page.waitForTimeout(1500);

        // Try to find the website link in the detail panel
        const website = await page.evaluate(() => {
          const link = document.querySelector('a[data-item-id="authority"]');
          return link ? link.getAttribute("href") : null;
        });

        businesses.push({
          name: raw[i].name,
          link: website || raw[i].link,
          emails: [],
        });
      } catch {
        businesses.push({
          name: raw[i].name,
          link: raw[i].link,
          emails: [],
        });
      }
    }

    await browser.close();
    browser = null;

    // Extract emails from each website (parallel, batched)
    console.log(`  [GMaps Scraper] extracting emails from ${businesses.length} websites...`);
    const BATCH = 5;
    for (let i = 0; i < businesses.length; i += BATCH) {
      const batch = businesses.slice(i, i + BATCH);
      const results = await Promise.allSettled(
        batch.map((b) => {
          if (b.link && b.link.startsWith("http") && !b.link.includes("google.com/maps")) {
            return extractEmails(b.link);
          }
          return Promise.resolve([]);
        })
      );
      results.forEach((r, j) => {
        if (r.status === "fulfilled") batch[j].emails = r.value;
      });
    }

    const withEmails = businesses.filter((b) => b.emails.length > 0).length;
    console.log(`  [GMaps Scraper] done: ${businesses.length} businesses, ${withEmails} with emails`);

    return businesses;

  } catch (err) {
    console.error(`  [GMaps Scraper] error: ${err.message}`);
    return [];
  } finally {
    if (browser) {
      try { await browser.close(); } catch {}
    }
  }
}

module.exports = scrapeGoogleMapsWithEmails;
