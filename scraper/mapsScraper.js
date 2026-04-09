const puppeteer = require("puppeteer");

/**
 * Scrape Google Maps for businesses matching a niche + location query.
 *
 * Opens a headless browser, searches Google Maps, scrolls the results panel
 * to load 30–50 businesses, then extracts name + rating from each card.
 *
 * @param {string} niche    - e.g. "dental clinic"
 * @param {string} location - e.g. "Sydney Australia"
 * @param {object} opts
 * @param {number} opts.maxResults  - stop scrolling after this many (default 40)
 * @param {number} opts.timeoutMs   - overall timeout in ms (default 30000)
 * @returns {Promise<Array<{name: string, rating: string|null}>>}
 */
async function scrapeGoogleMaps(niche, location, opts = {}) {
  const { maxResults = 40, timeoutMs = 30000 } = opts;
  const query = `${niche} in ${location}`;
  const url = `https://www.google.com/maps/search/${encodeURIComponent(query)}`;

  console.log(`  [Maps Scraper] query: "${query}"`);
  console.log(`  [Maps Scraper] url: ${url}`);

  let browser;
  try {
    browser = await puppeteer.launch({
      headless: "new",
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",   // use /tmp instead of /dev/shm (Render has small shm)
        "--disable-gpu",
        "--disable-extensions",
        "--single-process",           // reduces memory on constrained servers
        "--no-zygote",
      ],
    });

    const page = await browser.newPage();
    await page.setViewport({ width: 1280, height: 900 });
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    );

    // Navigate with a timeout
    await page.goto(url, { waitUntil: "networkidle2", timeout: timeoutMs });

    // Wait for the results feed to appear (the scrollable panel)
    const FEED_SELECTOR = 'div[role="feed"]';
    try {
      await page.waitForSelector(FEED_SELECTOR, { timeout: 10000 });
    } catch {
      console.log("  [Maps Scraper] No results feed found — possibly no results or CAPTCHA");
      return [];
    }

    // Scroll the results panel to load more businesses
    let previousCount = 0;
    let staleRounds = 0;
    const MAX_STALE = 3; // stop after 3 scrolls with no new results

    for (let scroll = 0; scroll < 15; scroll++) {
      // Scroll the feed panel down
      await page.evaluate((sel) => {
        const feed = document.querySelector(sel);
        if (feed) feed.scrollTop = feed.scrollHeight;
      }, FEED_SELECTOR);

      await page.waitForTimeout(1500);

      // Count current results
      const currentCount = await page.evaluate((sel) => {
        const feed = document.querySelector(sel);
        return feed ? feed.children.length : 0;
      }, FEED_SELECTOR);

      console.log(`  [Maps Scraper] scroll ${scroll + 1}: ${currentCount} results`);

      if (currentCount >= maxResults) break;
      if (currentCount === previousCount) {
        staleRounds++;
        if (staleRounds >= MAX_STALE) break;
      } else {
        staleRounds = 0;
      }
      previousCount = currentCount;
    }

    // Extract business data from result cards
    const businesses = await page.evaluate(() => {
      const results = [];
      // Each business card is an <a> with class "hfpxzc" inside the feed
      const cards = document.querySelectorAll('a.hfpxzc');

      for (const card of cards) {
        const name = card.getAttribute("aria-label") || "";
        if (!name) continue;

        // Rating is in a nearby span like "4.5" inside the card's parent
        const parent = card.closest('div[jsaction]') || card.parentElement;
        let rating = null;
        if (parent) {
          const ratingEl = parent.querySelector('span[role="img"]');
          if (ratingEl) {
            const match = ratingEl.getAttribute("aria-label")?.match(/([\d.]+)/);
            if (match) rating = match[1];
          }
        }

        results.push({ name: name.trim(), rating });
      }
      return results;
    });

    console.log(`  [Maps Scraper] extracted ${businesses.length} businesses`);
    return businesses.slice(0, maxResults);

  } catch (err) {
    console.error(`  [Maps Scraper] error: ${err.message}`);
    return [];
  } finally {
    if (browser) {
      try { await browser.close(); } catch {}
    }
  }
}

module.exports = { scrapeGoogleMaps };
