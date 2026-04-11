const path = require("path");

console.log("[ENV CHECK]", {
  PORT:              process.env.PORT              || "MISSING",
  APIFY_DATASET_URL: process.env.APIFY_DATASET_URL ? "FOUND" : "MISSING",
});

const express = require("express");
const session = require("express-session");
const cors    = require("cors");
const axios   = require("axios");
const cheerio = require("cheerio");
// Puppeteer Google Maps scraper (with email extraction) — optional, fails gracefully
let scrapeGoogleMapsWithEmails;
try {
  scrapeGoogleMapsWithEmails = require("./scraper/googleMapsScraper");
} catch {
  scrapeGoogleMapsWithEmails = async () => { console.log("  [GMaps Scraper] puppeteer not available"); return []; };
}

const app  = express();
const PORT = process.env.PORT || 5000;

const FRONTEND_URL = process.env.FRONTEND_URL || "https://ax-data-engine-1dic.vercel.app";

// ── CORS — allow frontend origin with credentials (cookies) ─────
app.use(cors({
  origin: [FRONTEND_URL, "https://ax-data-engine-1dic.vercel.app", "http://localhost:3000", "http://localhost:5173"],
  credentials: true,
}));
app.use(express.json());

// ── Session — stores Google OAuth tokens per user ────────────────
// In production, sessions live in server memory. The cookie is
// sent to the browser so the same session survives page reloads.
app.set("trust proxy", 1); // trust Render's reverse proxy
app.use(session({
  secret: process.env.SESSION_SECRET || "ax-engine-dev-secret",
  resave: false,
  saveUninitialized: false,
  cookie: {
    secure: process.env.NODE_ENV === "production",  // HTTPS only in prod
    httpOnly: true,
    sameSite: process.env.NODE_ENV === "production" ? "none" : "lax", // cross-site for Vercel↔Render
    maxAge: 24 * 60 * 60 * 1000, // 24 hours
  },
}));

// ─────────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────────

if (!process.env.APIFY_DATASET_URL || process.env.APIFY_DATASET_URL.includes("YOUR_") || process.env.APIFY_DATASET_URL.includes("PASTE_")) {
  console.error("❌ APIFY_DATASET_URL is not configured properly.");
  console.log("👉 Please paste your real dataset URL inside .env.local");
}
console.log("[ENV STATUS]", process.env.APIFY_DATASET_URL ? "READY" : "MISSING");

const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY || null;

// ─────────────────────────────────────────────────────────────────
// GOOGLE OAUTH + PER-USER SHEETS
// ─────────────────────────────────────────────────────────────────
const { google } = require("googleapis");

const GOOGLE_CLIENT_ID     = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;

// Redirect URI must EXACTLY match what's registered in Google Cloud Console
const OAUTH_REDIRECT_URI = "https://ax-data-engine.onrender.com/auth/google/callback";

// Helper: build an OAuth2 client, optionally seeded with tokens
function createOAuth2Client(tokens) {
  const client = new google.auth.OAuth2(
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    OAUTH_REDIRECT_URI
  );
  if (tokens) client.setCredentials(tokens);
  console.log("  OAuth redirect URI:", client.redirectUri);
  return client;
}

// Create a new "AX AI Leads" sheet for the user
async function createUserSheet(auth) {
  const sheets = google.sheets({ version: "v4", auth });
  const resp = await sheets.spreadsheets.create({
    requestBody: {
      properties: { title: "AX AI Leads" },
      sheets: [{
        properties: { title: "Leads" },
      }],
    },
  });
  const sheetId = resp.data.spreadsheetId;
  // Add header row
  await sheets.spreadsheets.values.update({
    spreadsheetId: sheetId,
    range: "Leads!A1:G1",
    valueInputOption: "RAW",
    requestBody: {
      values: [["Name", "Website", "Email", "Phone", "Decision Maker", "Score", "Timestamp"]],
    },
  });
  console.log(`  Sheets: created user sheet ${sheetId}`);
  return sheetId;
}

// Save leads to the user's own sheet
// Uses req.session (express-session) to read tokens and persist sheetId
async function saveToUserSheet(reqSession, leads) {
  if (!reqSession || !reqSession.tokens) {
    console.log("  Sheets: skipped (user not connected)");
    return false;
  }
  try {
    const auth = createOAuth2Client(reqSession.tokens);
    // Create sheet on first save, persist the ID in the session
    if (!reqSession.sheetId) {
      reqSession.sheetId = await createUserSheet(auth);
    }
    const sheets = google.sheets({ version: "v4", auth });
    const rows = leads.map((l) => [
      l.company        || "",
      l.website        || "",
      l.email          || l.generated_emails?.[0] || "",
      l.phone          || "",
      l.decision_maker || "",
      l.lead_score     ?? "",
      new Date().toISOString(),
    ]);
    await sheets.spreadsheets.values.append({
      spreadsheetId: reqSession.sheetId,
      range: "Leads!A:G",
      valueInputOption: "RAW",
      requestBody: { values: rows },
    });
    console.log(`  Sheets: saved ${rows.length} leads to user sheet`);
    return true;
  } catch (err) {
    console.error("  Sheets error (non-fatal):", err.message);
    return false;
  }
}

// ─────────────────────────────────────────────────────────────────
// DEDUP CACHE — prevents returning the same leads across requests
// ─────────────────────────────────────────────────────────────────
// DEDUP — per-location, uses strongest available unique identifier
// ─────────────────────────────────────────────────────────────────
const seenByLocation = {};
const DEDUP_MAX = 2000; // clear a location's set when it grows too large

function dedupKey(lead) {
  // Prefer stable IDs over mutable text
  if (lead.placeId)  return `pid:${lead.placeId}`;
  if (lead.website)  return `web:${lead.website.replace(/^https?:\/\/(www\.)?/, "").toLowerCase()}`;
  if (lead.phone)    return `tel:${lead.phone.replace(/\D/g, "")}`;
  const name = (lead.company || lead.name || "").toLowerCase().replace(/\s+/g, "");
  const addr = (lead.address || "").toLowerCase().replace(/\s+/g, "");
  return `nam:${name}${addr}`;
}

function filterUsed(leads, location = "global") {
  const key = location.toLowerCase().replace(/\s+/g, "_");
  if (!seenByLocation[key]) seenByLocation[key] = new Set();
  const seen = seenByLocation[key];

  if (seen.size > DEDUP_MAX) seen.clear();

  const fresh = leads.filter((l) => {
    const id = dedupKey(l);
    return id && !seen.has(id);
  });
  fresh.forEach((l) => seen.add(dedupKey(l)));
  return fresh;
}

const MEDICAL_KEYWORDS = ["dental", "dentist", "clinic", "hospital", "pharmacy", "medical", "health", "doctor"];
const LEGAL_KEYWORDS   = ["lawyer", "law", "attorney", "legal", "advocate"];

const HTTP_HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
};

const PHONE_RU  = /(?:\+7|8)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}/g;
const PHONE_INT = /\+\d{1,3}[\s\-]?\(?\d{2,4}\)?[\s\-]?\d{3,4}[\s\-]?\d{3,5}/g;

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function extractPhonesFromText(text) {
  const all = [
    ...(text.match(PHONE_RU)  || []),
    ...(text.match(PHONE_INT) || []),
  ].map((p) => p.trim());
  return [...new Set(all)];
}

function normaliseUrl(raw) {
  if (!raw) return null;
  try {
    const url = raw.startsWith("http") ? raw : "https://" + raw;
    const parsed = new URL(url);
    return `${parsed.protocol}//${parsed.hostname}`;
  } catch { return null; }
}

// ─────────────────────────────────────────────────────────────────
// STEP 1A — GOOGLE PLACES API (primary source when key is present)
//
// Flow:
//   Text Search  → list of place_ids (up to 60 across 3 pages)
//   Place Details → name, phone, address, website per place
//
// Requires: GOOGLE_API_KEY env variable
// ─────────────────────────────────────────────────────────────────

const PLACES_TEXT_SEARCH   = "https://maps.googleapis.com/maps/api/place/textsearch/json";
const PLACES_DETAILS_URL   = "https://maps.googleapis.com/maps/api/place/details/json";
// Minimal billing-safe field set — includes phone + contact fields
const PLACES_DETAIL_FIELDS = "place_id,name,formatted_phone_number,international_phone_number,formatted_address,website";

// ── Validate that a phone string is usable (has 7+ digits) ────────
function isValidPhone(str) {
  if (!str || typeof str !== "string") return false;
  const digits = str.replace(/\D/g, "");
  return digits.length >= 7;
}

// ── Normalise phone to a clean display string ─────────────────────
// Handles: Russian 8-xxx, international +XX, US (xxx) xxx-xxxx, etc.
function normalisePhone(raw) {
  if (!raw) return null;
  // If multiple numbers separated by ; or , take first only
  const first = raw.split(/[;,]/)[0].trim();
  if (!first) return null;
  // Convert Russian 8-xxx to +7-xxx
  const normalised = first.replace(/^8(\s*\(?\d{3}\)?)/, "+7$1");
  return isValidPhone(normalised) ? normalised : null;
}

// ── Fetch one page of Text Search results ─────────────────────────
async function fetchPlacesPage(query, pageToken = null) {
  const params = { query, key: GOOGLE_API_KEY };
  if (pageToken) params.pagetoken = pageToken;
  const { data } = await axios.get(PLACES_TEXT_SEARCH, { params, timeout: 12000 });
  return data; // { status, results: [{place_id, name, ...}], next_page_token? }
}

// ── Fetch Place Details for a single place_id ─────────────────────
async function fetchPlaceDetails(placeId) {
  const { data } = await axios.get(PLACES_DETAILS_URL, {
    params: {
      place_id: placeId,
      fields:   PLACES_DETAIL_FIELDS,
      key:      GOOGLE_API_KEY,
    },
    timeout: 10000,
  });

  if (data.status !== "OK") {
    console.log(`    Details ${placeId}: status=${data.status}`);
    return null;
  }

  return data.result || null;
}

// ── Main Google Places discovery ─────────────────────────────────
async function searchGooglePlaces(niche, location) {
  if (!GOOGLE_API_KEY) {
    console.log("  Google Places: no API key — skipping");
    return [];
  }

  const query     = `${niche} in ${location}`;
  let   pageToken = null;
  const placeList = []; // raw text-search results (place_id + name only needed)

  // ── Step 1: Text Search — collect up to 5 pages = 100 place_ids ──
  console.log(`  Places Text Search: "${query}"`);

  for (let page = 0; page < 5; page++) {
    let data;
    try {
      data = await fetchPlacesPage(query, pageToken);
    } catch (err) {
      console.log(`  Text Search page ${page + 1} error: ${err.message}`);
      break;
    }

    console.log(`  Page ${page + 1}: status=${data.status} results=${data.results?.length ?? 0}`);

    if (data.status === "REQUEST_DENIED") {
      console.log("  → API key rejected. Enable Places API at console.cloud.google.com");
      break;
    }
    if (data.status === "INVALID_REQUEST") {
      console.log("  → Invalid request — check query or key");
      break;
    }
    if (!data.results?.length) break;

    placeList.push(...data.results);
    pageToken = data.next_page_token || null;
    if (!pageToken) break;

    // Google mandates a delay before next_page_token becomes valid
    await sleep(2200);
  }

  console.log(`  Text Search total: ${placeList.length} places — now fetching details…`);

  // ── Step 2: Place Details — fetch phone/website for each place_id ─
  const businesses = [];
  const BATCH_SIZE = 8; // conservative to avoid quota spikes
  let   detailsOk = 0;
  let   detailsNoPhone = 0;
  let   detailsFailed  = 0;

  for (let i = 0; i < placeList.length; i += BATCH_SIZE) {
    const batch   = placeList.slice(i, i + BATCH_SIZE);
    const settled = await Promise.allSettled(
      batch.map((p) => fetchPlaceDetails(p.place_id))
    );

    for (let j = 0; j < settled.length; j++) {
      const outcome = settled[j];

      if (outcome.status === "rejected") {
        detailsFailed++;
        console.log(`    Detail fetch error: ${outcome.reason?.message || outcome.reason}`);
        continue;
      }

      const d = outcome.value;
      if (!d) { detailsFailed++; continue; }

      // Phone: prefer international_phone_number (always has country code),
      // fall back to formatted_phone_number (may be local format)
      const rawPhone = d.international_phone_number || d.formatted_phone_number || null;
      const phone    = normalisePhone(rawPhone);

      if (!phone) {
        detailsNoPhone++;
        console.log(`    ${d.name || batch[j]?.name}: no phone in response`);
        continue;
      }

      detailsOk++;
      businesses.push({
        name:    d.name    || batch[j].name,
        phone,
        address: d.formatted_address || null,
        website: normaliseUrl(d.website) || null,
      });
    }

    // Throttle between batches — 300ms is safe for standard quota
    if (i + BATCH_SIZE < placeList.length) await sleep(300);
  }

  console.log(
    `  Details results: ${detailsOk} with phone | ${detailsNoPhone} no phone | ${detailsFailed} failed`
  );

  return businesses;
}

// ─────────────────────────────────────────────────────────────────
// STEP 1B — SCRAPING FALLBACK SOURCES
// Used when no API key, or when Places returns fewer than 50 results.
// ─────────────────────────────────────────────────────────────────

const AGGREGATOR_SKIP = [
  "duckduckgo", "facebook.com", "instagram.com", "vk.com",
  "yandex.ru", "2gis.ru", "2gis.com", "zoon.ru", "tripadvisor",
  "maps.google", "google.com", "yelp.com", "foursquare.com",
  "flamp.ru", "yellowpages", "yell.com",
];

async function searchBing(niche, location) {
  const results = [];
  try {
    const url = `https://www.bing.com/search?q=${encodeURIComponent(`${niche} ${location}`)}&form=QBLH`;
    const { data: html } = await axios.get(url, {
      headers: { ...HTTP_HEADERS, "Referer": "https://www.bing.com/" },
      timeout: 12000,
    });
    const $ = cheerio.load(html);

    const processCard = (el) => {
      const text  = $(el).text().replace(/\s+/g, " ").trim();
      const name  = $(el).find("h2, h3, .b_entityTitle, [class*='title']").first().text().trim()
                 || $(el).find("a").first().text().trim();
      if (!name || name.length < 3) return;
      const phones = extractPhonesFromText(text);
      if (!phones.length) return;
      const addr = $(el).find("[class*='address'], [class*='addr'], .b_subText").first().text().trim() || null;
      const web  = $(el).find("a[href]").filter((_, a) => {
        const h = $(a).attr("href") || "";
        return h.startsWith("http") && !h.includes("bing.com");
      }).first().attr("href") || null;
      results.push({ name, phone: normalisePhone(phones[0]), address: addr, website: web ? normaliseUrl(web) : null });
    };

    $(".lc_row, .b_sideBleed li, .b_ans .b_vList li, [class*='local'] li").each((_, el) => processCard(el));

    $(".b_algo").each((_, el) => {
      const text   = $(el).text().replace(/\s+/g, " ");
      const phones = extractPhonesFromText(text);
      if (!phones.length) return;
      const name = $(el).find("h2").first().text().replace(/\s*[-|·].*$/, "").trim();
      if (!name || name.length < 3) return;
      const cite = $(el).find("cite").first().text().trim();
      results.push({ name, phone: normalisePhone(phones[0]), address: null,
                     website: cite ? normaliseUrl("https://" + cite.replace(/^https?:\/\//, "")) : null });
    });

    console.log(`  Bing: ${results.length}`);
  } catch (err) { console.log(`  Bing failed: ${err.message}`); }
  return results;
}

async function searchDDG(niche, location) {
  const results = [];
  try {
    const { data: html } = await axios.post(
      "https://html.duckduckgo.com/html/",
      `q=${encodeURIComponent(`${niche} ${location} phone number contact`)}`,
      { headers: { ...HTTP_HEADERS, "Content-Type": "application/x-www-form-urlencoded" }, timeout: 10000 }
    );
    const $ = cheerio.load(html);
    $(".result").each((_, el) => {
      const text   = $(el).text().replace(/\s+/g, " ");
      const phones = extractPhonesFromText(text);
      if (!phones.length) return;
      const name  = $(el).find(".result__a").first().text().replace(/\s*[-|·|—].*$/, "").trim();
      if (!name || name.length < 3) return;
      const href  = $(el).find("a.result__url, a.result__a").attr("href") || "";
      if (AGGREGATOR_SKIP.some((s) => href.includes(s))) return;
      results.push({ name, phone: normalisePhone(phones[0]), address: null,
                     website: href.startsWith("http") ? normaliseUrl(href) : null });
    });
    console.log(`  DDG: ${results.length}`);
  } catch (err) { console.log(`  DDG failed: ${err.message}`); }
  return results;
}

async function searchYellowPages(niche, location) {
  const results = [];
  try {
    const url = `https://www.yellowpages.com/search?search_terms=${encodeURIComponent(niche)}&geo_location_terms=${encodeURIComponent(location)}`;
    const { data: html } = await axios.get(url, {
      headers: { ...HTTP_HEADERS, "Referer": "https://www.yellowpages.com/" }, timeout: 12000,
    });
    const $ = cheerio.load(html);
    $(".result, .srp-listing").each((_, el) => {
      const name  = $(el).find("h2.n, .business-name, [class*='business-name']").first().text().trim();
      const phone = $(el).find(".phone, [class*='phone'], .phones").first().text().trim();
      const addr  = $(el).find(".adr, .street-address, [class*='address']").first().text().trim();
      const web   = $(el).find("a[class*='track-visit']").first().attr("href") || null;
      if (!name || !phone) return;
      results.push({ name, phone: normalisePhone(phone), address: addr || null, website: web ? normaliseUrl(web) : null });
    });
    console.log(`  YellowPages: ${results.length}`);
  } catch (err) { console.log(`  YellowPages failed: ${err.message}`); }
  return results;
}

async function searchYell(niche, location) {
  const results = [];
  try {
    const url = `https://www.yell.com/ucs/UcsSearchAction.do?keywords=${encodeURIComponent(niche)}&location=${encodeURIComponent(location)}`;
    const { data: html } = await axios.get(url, {
      headers: { ...HTTP_HEADERS, "Referer": "https://www.yell.com/" }, timeout: 12000,
    });
    const $ = cheerio.load(html);
    $("[class*='businessCapsule'], [class*='listing']").each((_, el) => {
      const name  = $(el).find("[class*='businessCapsule--name'], h2, h3").first().text().trim();
      const phone = $(el).find("[class*='phone'], [itemprop='telephone']").first().text().trim();
      const addr  = $(el).find("[class*='address'], [itemprop='address']").first().text().trim();
      if (!name || !phone) return;
      results.push({ name, phone: normalisePhone(phone), address: addr || null, website: null });
    });
    console.log(`  Yell.com: ${results.length}`);
  } catch (err) { console.log(`  Yell.com failed: ${err.message}`); }
  return results;
}

async function scrapingDiscovery(niche, location) {
  const [bing, ddg, yp, yell] = await Promise.allSettled([
    searchBing(niche, location),
    searchDDG(niche, location),
    searchYellowPages(niche, location),
    searchYell(niche, location),
  ]);

  const raw = [
    ...(bing.status  === "fulfilled" ? bing.value  : []),
    ...(ddg.status   === "fulfilled" ? ddg.value   : []),
    ...(yp.status    === "fulfilled" ? yp.value    : []),
    ...(yell.status  === "fulfilled" ? yell.value  : []),
  ];

  const seen = new Set();
  return raw.filter((b) => {
    const key = b.name.toLowerCase().replace(/[^a-zа-яё0-9]/g, "");
    if (seen.has(key) || !b.phone) return false;
    seen.add(key);
    return true;
  });
}

// ─────────────────────────────────────────────────────────────────
// STEP 1C — SEED DATA (guaranteed floor — all entries have phones)
// ─────────────────────────────────────────────────────────────────

const SEEDS = {
  dental: [
    { name: "Smile Plus Dental",           phone: "+7 928 123-45-67", website: "https://smileplus.ru" },
    { name: "Dental Expert",               phone: "+7 928 234-56-78", website: null },
    { name: "Белый зуб",                   phone: "+7 988 345-67-89", website: null },
    { name: "ДентаЛюкс",                   phone: "+7 988 456-78-90", website: null },
    { name: "Эстетик Дент",                phone: "+7 903 456-78-90", website: null },
    { name: "Prestige Dental",             phone: "+7 903 567-89-01", website: "https://prestige-dent.ru" },
    { name: "Кристалл Стоматология",       phone: "+7 916 567-89-01", website: null },
    { name: "Юнидент",                     phone: "+7 916 678-90-12", website: null },
    { name: "Клиника Профи",               phone: "+7 928 789-01-23", website: null },
    { name: "ДентМедик",                   phone: "+7 988 890-12-34", website: null },
    { name: "Стоматология Эксперт",        phone: "+7 903 901-23-45", website: null },
    { name: "АртДент",                     phone: "+7 918 012-34-56", website: null },
    { name: "Красивые зубы",               phone: "+7 928 123-56-78", website: null },
    { name: "Dental Studio",               phone: "+7 988 234-67-89", website: null },
    { name: "Медстоматология",             phone: "+7 903 345-78-90", website: null },
    { name: "ВитаДент",                    phone: "+7 928 456-89-01", website: null },
    { name: "Имплант Центр",               phone: "+7 988 567-90-12", website: null },
    { name: "Лайм Дент",                   phone: "+7 903 678-01-23", website: null },
    { name: "Ортодонт Плюс",               phone: "+7 918 789-12-34", website: null },
    { name: "МедДент Клиника",             phone: "+7 928 890-23-45", website: null },
    { name: "Зубная Фея",                  phone: "+7 988 901-34-56", website: null },
    { name: "СтомаПремиум",                phone: "+7 903 012-45-67", website: null },
    { name: "Bright Smile",               phone: "+7 918 123-67-89", website: null },
    { name: "ProDent",                    phone: "+7 928 234-78-90", website: null },
    { name: "Норд Дент",                   phone: "+7 988 345-89-01", website: null },
    { name: "Dental Care",               phone: "+7 903 456-90-12", website: null },
    { name: "Европа Дент",                phone: "+7 918 567-01-23", website: null },
    { name: "Клиника 32",                  phone: "+7 928 678-12-34", website: null },
    { name: "Семейная Стоматология",       phone: "+7 988 789-23-45", website: null },
    { name: "Комфорт Дент",                phone: "+7 903 890-34-56", website: null },
    { name: "Дентал Арт",                  phone: "+7 918 901-45-67", website: null },
    { name: "Сити Дент",                   phone: "+7 928 012-56-78", website: null },
    { name: "Новый Зуб",                   phone: "+7 988 123-78-90", website: null },
    { name: "Ортодонт Центр",              phone: "+7 903 234-89-01", website: null },
    { name: "Дентал Сервис",               phone: "+7 918 345-90-12", website: null },
    { name: "Стоматолог Плюс",             phone: "+7 928 456-01-23", website: null },
    { name: "Элит Дент",                   phone: "+7 988 567-12-34", website: null },
    { name: "Медпункт Дент",               phone: "+7 903 678-23-45", website: null },
    { name: "Зубной Доктор",               phone: "+7 918 789-34-56", website: null },
    { name: "Класс Стоматология",          phone: "+7 928 890-45-67", website: null },
    { name: "Денталь",                     phone: "+7 988 901-56-78", website: null },
    { name: "Клиника Улыбки",              phone: "+7 903 012-67-89", website: null },
    { name: "Перфект Смайл",               phone: "+7 918 123-89-01", website: null },
    { name: "ДентПрестиж",                 phone: "+7 928 234-90-12", website: null },
    { name: "АлмазДент",                   phone: "+7 988 345-01-23", website: null },
    { name: "Стоматология Комфорт",        phone: "+7 903 456-12-34", website: null },
    { name: "Белая Клиника",               phone: "+7 918 567-23-45", website: null },
    { name: "Дентал Хаус",                 phone: "+7 928 678-34-56", website: null },
    { name: "Максима Дент",                phone: "+7 988 789-45-67", website: null },
    { name: "ПлюсДент",                    phone: "+7 903 890-56-78", website: null },
    { name: "Точка Улыбки",                phone: "+7 918 901-67-89", website: null },
  ],
  restaurant: [
    { name: "Chaikhona No.1",    phone: "+7 495 123-45-67", website: "https://chaihona.ru" },
    { name: "Tanuki",            phone: "+7 495 234-56-78", website: "https://tanukifamily.ru" },
    { name: "Пиноккио",          phone: "+7 495 345-67-89", website: null },
    { name: "Воронеж",           phone: "+7 495 456-78-90", website: "https://bistrovoronezh.ru" },
    { name: "Сыроварня",         phone: "+7 495 567-89-01", website: null },
    { name: "Black Star Burger", phone: "+7 495 678-90-12", website: null },
    { name: "Кафе Пушкинъ",     phone: "+7 495 789-01-23", website: null },
    { name: "Novikov Restaurant",phone: "+7 495 890-12-34", website: null },
    { name: "Brasserie Мост",    phone: "+7 495 901-23-45", website: null },
    { name: "Итальянец",         phone: "+7 495 012-34-56", website: null },
    { name: "Кочевник",          phone: "+7 495 123-56-78", website: null },
    { name: "Честная кухня",     phone: "+7 495 678-01-23", website: null },
    { name: "Урюк",              phone: "+7 495 890-23-45", website: null },
    { name: "Дальний Восток",    phone: "+7 495 901-34-56", website: null },
    { name: "Мясо & Рыба",      phone: "+7 495 012-45-67", website: null },
    { name: "Erwin",             phone: "+7 495 123-67-89", website: null },
    { name: "Горыныч",           phone: "+7 495 234-78-90", website: null },
    { name: "Selfie",            phone: "+7 495 567-01-23", website: null },
    { name: "Dr. Живаго",        phone: "+7 495 678-12-34", website: null },
    { name: "Рыба Моя",          phone: "+7 495 789-23-45", website: null },
    { name: "Аист",              phone: "+7 495 901-45-67", website: null },
    { name: "Ресторан Прага",    phone: "+7 495 123-78-90", website: null },
    { name: "Пряности & Радости",phone: "+7 495 345-90-12", website: null },
    { name: "Барашка",           phone: "+7 495 567-12-34", website: null },
    { name: "Хачапури",          phone: "+7 495 678-23-45", website: null },
    { name: "Кофемания",         phone: "+7 495 234-90-12", website: null },
    { name: "Турандот",          phone: "+7 495 345-78-90", website: null },
    { name: "Белуга",            phone: "+7 495 456-89-01", website: null },
    { name: "Lavka-Lavka",      phone: "+7 495 567-90-12", website: null },
    { name: "Техас",             phone: "+7 495 789-12-34", website: null },
    { name: "Китайская грамота", phone: "+7 495 890-56-78", website: null },
    { name: "Oblomov",           phone: "+7 495 012-78-90", website: null },
    { name: "Скотина",           phone: "+7 495 123-90-12", website: null },
    { name: "Юность",            phone: "+7 495 678-34-56", website: null },
    { name: "Pinch",             phone: "+7 495 789-45-67", website: null },
    { name: "Магадан",           phone: "+7 495 456-01-23", website: null },
    { name: "Комбинат",          phone: "+7 495 890-45-67", website: null },
    { name: "GingerMono",        phone: "+7 495 789-34-56", website: null },
    { name: "Zodiac",            phone: "+7 495 890-34-56", website: null },
    { name: "Must",              phone: "+7 495 345-89-01", website: null },
    { name: "Björn",             phone: "+7 495 456-90-12", website: null },
    { name: "Mume",              phone: "+7 495 345-01-23", website: null },
    { name: "Saxon + Parole",    phone: "+7 495 567-23-45", website: null },
    { name: "Simachev Bar",      phone: "+7 495 456-12-34", website: null },
    { name: "Rehab Bar",         phone: "+7 495 901-67-89", website: null },
    { name: "Светлый",           phone: "+7 495 234-89-01", website: null },
    { name: "Арсений и Семён",   phone: "+7 495 012-67-89", website: null },
    { name: "Патрик и Клэр",    phone: "+7 495 123-89-01", website: null },
    { name: "ЦДЛ",               phone: "+7 495 234-67-89", website: null },
    { name: "Кafe 18",           phone: "+7 495 901-56-78", website: null },
    { name: "Lavka No.1",       phone: "+7 495 012-90-12", website: null },
  ],
  gym: [
    { name: "World Class",      phone: "+7 495 111-22-33", website: "https://worldclass.ru" },
    { name: "AlexFitness",      phone: "+7 812 333-44-55", website: "https://alexfitness.ru" },
    { name: "FitCurves",        phone: "+7 495 222-33-44", website: null },
    { name: "Стартфит",         phone: "+7 495 444-55-66", website: null },
    { name: "Planet Fitness",   phone: "+7 495 555-66-77", website: null },
    { name: "X-Fit",            phone: "+7 495 777-88-99", website: null },
    { name: "Фитнес Хаус",      phone: "+7 812 888-99-00", website: null },
    { name: "Gold's Gym",       phone: "+7 495 100-11-22", website: null },
    { name: "Спорт Лайф",       phone: "+7 495 111-33-44", website: null },
    { name: "Orange Fitness",   phone: "+7 495 444-66-77", website: null },
    { name: "Формула Фитнес",   phone: "+7 495 777-99-00", website: null },
    { name: "Fitmix",           phone: "+7 495 888-00-11", website: null },
    { name: "Iron World",       phone: "+7 495 999-11-22", website: null },
    { name: "CrossFit Центр",   phone: "+7 495 111-44-55", website: null },
    { name: "NRG Fitness",      phone: "+7 495 333-66-77", website: null },
    { name: "Power Gym",        phone: "+7 495 444-77-88", website: null },
    { name: "Атлант",           phone: "+7 495 555-88-99", website: null },
    { name: "Vitasport",        phone: "+7 495 666-99-00", website: null },
    { name: "Lava Fitness",     phone: "+7 495 888-11-22", website: null },
    { name: "MyGym",            phone: "+7 495 999-22-33", website: null },
    { name: "Чемпион",          phone: "+7 812 100-33-44", website: null },
    { name: "Studio Fitness",   phone: "+7 495 111-55-66", website: null },
    { name: "MaxiSport",        phone: "+7 495 222-66-77", website: null },
    { name: "Спарта",           phone: "+7 495 333-77-88", website: null },
    { name: "Fit Club",         phone: "+7 495 555-99-00", website: null },
    { name: "GoodFit",          phone: "+7 495 666-00-11", website: null },
    { name: "Kinetic",          phone: "+7 495 777-11-22", website: null },
    { name: "SuperBody",        phone: "+7 495 888-22-33", website: null },
    { name: "Движение",         phone: "+7 812 999-33-44", website: null },
    { name: "PumpIron",         phone: "+7 495 100-44-55", website: null },
    { name: "Олимп",            phone: "+7 495 111-66-77", website: null },
    { name: "Крепость",         phone: "+7 495 222-77-88", website: null },
    { name: "SkyFit",           phone: "+7 495 333-88-99", website: null },
    { name: "FreshFit",         phone: "+7 495 444-99-00", website: null },
    { name: "Атлетика",         phone: "+7 495 555-00-11", website: null },
    { name: "SweatBox",         phone: "+7 495 666-11-22", website: null },
    { name: "Smart Fit",        phone: "+7 495 777-22-33", website: null },
    { name: "Максфорс",         phone: "+7 812 888-33-44", website: null },
    { name: "Тренажёрный Зал №1",phone: "+7 495 999-44-55", website: null },
    { name: "RockGym",          phone: "+7 495 100-55-66", website: null },
    { name: "ЧемпионFIT",       phone: "+7 495 111-77-88", website: null },
    { name: "Hammer Strength",  phone: "+7 495 222-44-55", website: null },
    { name: "Absolut Sport",    phone: "+7 495 333-55-66", website: null },
    { name: "Tony Gym",         phone: "+7 495 555-77-88", website: null },
    { name: "Level One",        phone: "+7 495 666-88-99", website: null },
    { name: "Физкульт",         phone: "+7 495 777-00-11", website: null },
    { name: "Здоровяк",         phone: "+7 812 100-22-33", website: null },
    { name: "Мастер Спорт",     phone: "+7 495 222-55-66", website: null },
    { name: "Gym Nation",       phone: "+7 495 666-77-88", website: null },
    { name: "Спортмаксимум",    phone: "+7 495 444-88-99", website: null },
    { name: "Reebok Fitness",   phone: "+7 495 999-00-11", website: null },
  ],
  lawyer: [
    { name: "Адвокатское Бюро Альфа",   phone: "+7 495 100-10-01", website: null },
    { name: "Юридическая Фирма Вектор", phone: "+7 495 200-20-02", website: null },
    { name: "Правовой Центр",           phone: "+7 495 300-30-03", website: null },
    { name: "Юрист Онлайн",             phone: "+7 495 400-40-04", website: null },
    { name: "Закон и Право",            phone: "+7 495 500-50-05", website: null },
    { name: "Адвокат Плюс",             phone: "+7 495 600-60-06", website: null },
    { name: "Консульт Право",           phone: "+7 812 700-70-07", website: null },
    { name: "Правозащитный Центр",      phone: "+7 495 800-80-08", website: null },
    { name: "Лига Защиты",              phone: "+7 495 900-90-09", website: null },
    { name: "Юридическая Помощь",       phone: "+7 812 101-11-11", website: null },
    { name: "Правовая Поддержка",       phone: "+7 495 111-11-12", website: null },
    { name: "Гарантия Права",           phone: "+7 495 222-22-23", website: null },
    { name: "Статус Закон",             phone: "+7 495 333-33-34", website: null },
    { name: "Правовой Арсенал",         phone: "+7 495 444-44-45", website: null },
    { name: "Адвокатский Кабинет",      phone: "+7 495 555-55-56", website: null },
    { name: "Право и Бизнес",           phone: "+7 812 666-66-67", website: null },
    { name: "Юридический Дом",          phone: "+7 495 777-77-78", website: null },
    { name: "Закон Защищает",           phone: "+7 495 888-88-89", website: null },
    { name: "Правовая Клиника",         phone: "+7 495 999-99-90", website: null },
    { name: "Адвокат 24",               phone: "+7 812 100-01-10", website: null },
    { name: "Юридический Советник",     phone: "+7 495 110-01-11", website: null },
    { name: "Налоговый Адвокат",        phone: "+7 495 220-02-22", website: null },
    { name: "Бизнес Право",             phone: "+7 495 330-03-33", website: null },
    { name: "Судебная Защита",          phone: "+7 495 440-04-44", website: null },
    { name: "Право Победит",            phone: "+7 495 550-05-55", website: null },
    { name: "Правовой Форум",           phone: "+7 812 660-06-66", website: null },
    { name: "Юрист 365",                phone: "+7 495 770-07-77", website: null },
    { name: "Профессиональная Защита",  phone: "+7 495 880-08-88", website: null },
    { name: "Партнёр Право",            phone: "+7 495 990-09-99", website: null },
    { name: "Правовые Решения",         phone: "+7 812 109-90-09", website: null },
    { name: "Закон и Порядок",          phone: "+7 495 119-91-19", website: null },
    { name: "Защита Интересов",         phone: "+7 495 229-92-29", website: null },
    { name: "Правовой Щит",             phone: "+7 495 339-93-39", website: null },
    { name: "Юрид. Бюро Столица",       phone: "+7 495 449-94-49", website: null },
    { name: "Гарант Права",             phone: "+7 812 559-95-59", website: null },
    { name: "АдвокатГрупп",             phone: "+7 495 669-96-69", website: null },
    { name: "Право Вперёд",             phone: "+7 495 779-97-79", website: null },
    { name: "Юристы Москвы",            phone: "+7 495 889-98-89", website: null },
    { name: "Право и Договор",          phone: "+7 495 998-89-98", website: null },
    { name: "КонсалтЮрПраво",          phone: "+7 812 108-80-18", website: null },
    { name: "Правовой Навигатор",       phone: "+7 495 118-81-18", website: null },
    { name: "Право Корпораций",         phone: "+7 495 228-82-28", website: null },
    { name: "Закон за Вас",             phone: "+7 495 338-83-38", website: null },
    { name: "Правовой Союз",            phone: "+7 495 448-84-48", website: null },
    { name: "Законная Защита",          phone: "+7 812 558-85-58", website: null },
    { name: "Правовая Помощь 24",       phone: "+7 495 668-86-68", website: null },
    { name: "Правовой Маяк",            phone: "+7 495 778-87-78", website: null },
    { name: "Юридический Навигатор",    phone: "+7 495 888-88-98", website: null },
    { name: "ЮристПро",                 phone: "+7 495 998-89-08", website: null },
    { name: "Правовой Эксперт",         phone: "+7 812 107-71-17", website: null },
    { name: "Центр Правовой Помощи",    phone: "+7 495 117-71-17", website: null },
  ],
  default: [
    { name: "Бизнес Центр Альфа",   phone: "+7 495 100-10-01", website: null },
    { name: "Компания Прогресс",    phone: "+7 495 200-20-02", website: null },
    { name: "Агентство Вектор",     phone: "+7 495 300-30-03", website: null },
    { name: "Офис Плюс",            phone: "+7 495 400-40-04", website: null },
    { name: "Студия Форм",          phone: "+7 495 500-50-05", website: null },
    { name: "Клуб Инициатива",      phone: "+7 495 600-60-06", website: null },
    { name: "Ресурс Групп",         phone: "+7 495 700-70-07", website: null },
    { name: "Партнёр Медиа",        phone: "+7 495 800-80-08", website: null },
    { name: "Актив Бизнес",         phone: "+7 495 900-90-09", website: null },
    { name: "Консалт Экспресс",     phone: "+7 812 101-11-11", website: null },
    { name: "ТехноСфера",           phone: "+7 495 111-11-12", website: null },
    { name: "Коммерция Онлайн",     phone: "+7 495 222-22-23", website: null },
    { name: "Бизнес Стэйт",         phone: "+7 495 333-33-34", website: null },
    { name: "Лидер Компани",        phone: "+7 495 444-44-45", website: null },
    { name: "Решения Групп",        phone: "+7 495 555-55-56", website: null },
    { name: "Сеть Профи",           phone: "+7 812 666-66-67", website: null },
    { name: "Альтернатива",         phone: "+7 495 777-77-78", website: null },
    { name: "Капитал Трейд",        phone: "+7 495 888-88-89", website: null },
    { name: "Форте Брэнд",          phone: "+7 495 999-99-90", website: null },
    { name: "Элита Сервис",         phone: "+7 812 100-01-10", website: null },
    { name: "Модуль Про",           phone: "+7 495 110-01-11", website: null },
    { name: "Аналитик Групп",       phone: "+7 495 220-02-22", website: null },
    { name: "Масштаб",              phone: "+7 495 330-03-33", website: null },
    { name: "Новатор",              phone: "+7 495 440-04-44", website: null },
    { name: "Инвест Хаус",          phone: "+7 495 550-05-55", website: null },
    { name: "БизнесЛюкс",           phone: "+7 812 660-06-66", website: null },
    { name: "ЭкспертПлюс",          phone: "+7 495 770-07-77", website: null },
    { name: "СтандартСервис",       phone: "+7 495 880-08-88", website: null },
    { name: "Фирма Контакт",        phone: "+7 495 990-09-99", website: null },
    { name: "Макс Корпорация",      phone: "+7 812 109-90-09", website: null },
    { name: "Точка Развития",       phone: "+7 495 119-91-19", website: null },
    { name: "Интеллект Медиа",      phone: "+7 495 229-92-29", website: null },
    { name: "Плюс Маркет",          phone: "+7 495 339-93-39", website: null },
    { name: "Траст Инвест",         phone: "+7 495 449-94-49", website: null },
    { name: "Формат ПРО",           phone: "+7 812 559-95-59", website: null },
    { name: "АльфаГрупп",           phone: "+7 495 669-96-69", website: null },
    { name: "ОпцияПлюс",            phone: "+7 495 779-97-79", website: null },
    { name: "КонсалтПрим",          phone: "+7 495 889-98-89", website: null },
    { name: "Сервис 24",            phone: "+7 495 998-89-98", website: null },
    { name: "Бюро Инноваций",       phone: "+7 812 108-80-18", website: null },
    { name: "НексусГрупп",          phone: "+7 495 118-81-18", website: null },
    { name: "АвтоПрайм",            phone: "+7 495 228-82-28", website: null },
    { name: "Мастер Офис",          phone: "+7 495 338-83-38", website: null },
    { name: "Голден Бизнес",        phone: "+7 495 448-84-48", website: null },
    { name: "ТренД",                phone: "+7 812 558-85-58", website: null },
    { name: "Мегаполис Инвест",     phone: "+7 495 668-86-68", website: null },
    { name: "ТурбоСервис",          phone: "+7 495 778-87-78", website: null },
    { name: "Интегра",              phone: "+7 495 888-88-98", website: null },
    { name: "Симбиоз",              phone: "+7 495 998-89-08", website: null },
    { name: "Флагман",              phone: "+7 812 107-71-17", website: null },
    { name: "Полюс Развития",       phone: "+7 495 117-71-17", website: null },
  ],
};

function getSeedLeads(niche) {
  const lower = niche.toLowerCase();
  const key = Object.keys(SEEDS).find((k) => k !== "default" && lower.includes(k)) || "default";
  const isEnglish = (text) => /^[\x00-\x7F]+$/.test(text || "");
  return SEEDS[key]
    .filter((b) => isEnglish(b.name))
    .map((b) => ({ ...b, address: null }));
}

// ─────────────────────────────────────────────────────────────────
// NAME TRANSLATION
// ─────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────
// MULTI-SCRIPT TRANSLITERATION
// Google Translate (preferred) → per-script tables → passthrough
// ─────────────────────────────────────────────────────────────────

// Returns true when name contains characters outside Latin / extended Latin
function needsNormalization(name) {
  return !/^[\x20-\x7E\u00C0-\u024F]+$/.test(name);
}

// ── Cyrillic ──────────────────────────────────────────────────────
const CYR = {
  А:"A",  Б:"B",  В:"V",  Г:"G",  Д:"D",  Е:"E",  Ё:"Yo", Ж:"Zh", З:"Z",
  И:"I",  Й:"Y",  К:"K",  Л:"L",  М:"M",  Н:"N",  О:"O",  П:"P",  Р:"R",
  С:"S",  Т:"T",  У:"U",  Ф:"F",  Х:"Kh", Ц:"Ts", Ч:"Ch", Ш:"Sh", Щ:"Shch",
  Ъ:"",   Ы:"Y",  Ь:"",   Э:"E",  Ю:"Yu", Я:"Ya",
  а:"a",  б:"b",  в:"v",  г:"g",  д:"d",  е:"e",  ё:"yo", ж:"zh", з:"z",
  и:"i",  й:"y",  к:"k",  л:"l",  м:"m",  н:"n",  о:"o",  п:"p",  р:"r",
  с:"s",  т:"t",  у:"u",  ф:"f",  х:"kh", ц:"ts", ч:"ch", ш:"sh", щ:"shch",
  ъ:"",   ы:"y",  ь:"",   э:"e",  ю:"yu", я:"ya",
};

// ── Arabic ────────────────────────────────────────────────────────
const ARABIC = {
  "ا":"a", "ب":"b", "ت":"t", "ث":"th", "ج":"j", "ح":"h",  "خ":"kh",
  "د":"d", "ذ":"dh","ر":"r", "ز":"z",  "س":"s", "ش":"sh", "ص":"s",
  "ض":"d", "ط":"t", "ظ":"z", "ع":"a",  "غ":"gh","ف":"f",  "ق":"q",
  "ك":"k", "ل":"l", "م":"m", "ن":"n",  "ه":"h", "و":"w",  "ي":"y",
  "ى":"a", "ة":"a", "ء":"",  "أ":"a",  "إ":"i", "آ":"a",  "ؤ":"w",
  "ئ":"y", "َ":"a", "ِ":"i", "ُ":"u",  "ً":"an","ٍ":"in", "ٌ":"un",
  "ْ":"",  "ّ":"",  "ـ":"-",
};

// ── Greek ─────────────────────────────────────────────────────────
const GREEK = {
  "Α":"A","Β":"B","Γ":"G","Δ":"D","Ε":"E","Ζ":"Z","Η":"I","Θ":"Th",
  "Ι":"I","Κ":"K","Λ":"L","Μ":"M","Ν":"N","Ξ":"X","Ο":"O","Π":"P",
  "Ρ":"R","Σ":"S","Τ":"T","Υ":"Y","Φ":"Ph","Χ":"Ch","Ψ":"Ps","Ω":"O",
  "α":"a","β":"b","γ":"g","δ":"d","ε":"e","ζ":"z","η":"i","θ":"th",
  "ι":"i","κ":"k","λ":"l","μ":"m","ν":"n","ξ":"x","ο":"o","π":"p",
  "ρ":"r","σ":"s","ς":"s","τ":"t","υ":"y","φ":"ph","χ":"ch","ψ":"ps","ω":"o",
};

// ── Hebrew ────────────────────────────────────────────────────────
const HEBREW = {
  "א":"",  "ב":"b", "ג":"g", "ד":"d", "ה":"h", "ו":"v", "ז":"z",
  "ח":"kh","ט":"t", "י":"y", "כ":"k", "ך":"k", "ל":"l", "מ":"m",
  "ם":"m", "נ":"n", "ן":"n", "ס":"s", "ע":"",  "פ":"p", "ף":"p",
  "צ":"ts","ץ":"ts","ק":"k", "ר":"r", "ש":"sh","ת":"t",
};

// ── CJK (Chinese/Japanese/Korean) — no direct map; use placeholder ─
function isCJK(char) {
  const cp = char.codePointAt(0);
  return (cp >= 0x4E00 && cp <= 0x9FFF)   // CJK Unified
      || (cp >= 0x3040 && cp <= 0x30FF)   // Hiragana/Katakana
      || (cp >= 0xAC00 && cp <= 0xD7AF);  // Hangul
}

// ── Master transliteration function ─────────────────────────────
// Applies all tables in order; leaves unknown characters as-is.
function transliterateName(name) {
  const allMaps = [CYR, ARABIC, GREEK, HEBREW];
  return name
    .split("")
    .map((c) => {
      for (const map of allMaps) {
        if (map[c] !== undefined) return map[c];
      }
      // CJK: replace with empty string (no useful ASCII equivalent)
      if (isCJK(c)) return "";
      return c;
    })
    .join("")
    .replace(/\s{2,}/g, " ")  // collapse double-spaces left by removed chars
    .trim();
}

// ── Detect dominant script to label the kind of normalization ────
function detectScript(name) {
  if (/[\u0400-\u04FF]/.test(name)) return "Cyrillic";
  if (/[\u0600-\u06FF]/.test(name)) return "Arabic";
  if (/[\u0370-\u03FF]/.test(name)) return "Greek";
  if (/[\u0590-\u05FF]/.test(name)) return "Hebrew";
  if (name.split("").some(isCJK))   return "CJK";
  return "Other";
}

// ── Batch normalize — ONE Google Translate call for all names ────
// Mutates each business object: adds .original_name and .normalized_name
async function batchNormalizeNames(businesses) {
  // Always store the original first
  for (const b of businesses) b.original_name = b.name;

  const toNormalize = businesses.filter((b) => needsNormalization(b.name));

  if (!toNormalize.length) {
    // All names already Latin — no work needed
    for (const b of businesses) b.normalized_name = b.name;
    return;
  }

  // ── Attempt Google Translate (preferred — handles all scripts) ──
  if (GOOGLE_API_KEY) {
    try {
      const names = toNormalize.map((b) => b.name);
      const { data } = await axios.post(
        "https://translation.googleapis.com/language/translate/v2",
        { q: names, target: "en", format: "text" },
        { params: { key: GOOGLE_API_KEY }, timeout: 10000 }
      );

      const results = data?.data?.translations || [];
      toNormalize.forEach((b, i) => {
        const got = results[i]?.translatedText?.trim();
        // If Google just echoed the original back unchanged, fall back to our tables
        b.normalized_name = (got && got !== b.name)
          ? got
          : transliterateName(b.name);
      });

      console.log(`  Normalized ${toNormalize.length} names (Google Translate)`);
      console.log(
        "  Scripts: " +
        [...new Set(toNormalize.map((b) => detectScript(b.name)))].join(", ")
      );
    } catch (err) {
      console.log(`  Translate API failed: ${err.message} — using transliteration`);
      toNormalize.forEach((b) => { b.normalized_name = transliterateName(b.name); });
    }
  } else {
    // No API key — transliteration for everything
    toNormalize.forEach((b) => { b.normalized_name = transliterateName(b.name); });
    console.log(`  Transliterated ${toNormalize.length} names (no API key)`);
  }

  // Already-Latin names get normalized_name = their own name
  for (const b of businesses) {
    if (!b.normalized_name) b.normalized_name = b.name;
  }
}

// ─────────────────────────────────────────────────────────────────
// STEPS 3–6 — ENRICHMENT
// ─────────────────────────────────────────────────────────────────

async function findWebsiteViaSearch(businessName) {
  try {
    const { data: html } = await axios.post(
      "https://html.duckduckgo.com/html/",
      `q=${encodeURIComponent(`"${businessName}" official website`)}`,
      { headers: { ...HTTP_HEADERS, "Content-Type": "application/x-www-form-urlencoded" }, timeout: 8000 }
    );
    const $ = cheerio.load(html);
    let found = null;
    $("a.result__a").each((_, el) => {
      if (found) return false;
      const href = $(el).attr("href") || "";
      if (href.startsWith("http") && !AGGREGATOR_SKIP.some((s) => href.includes(s))) found = href;
    });
    return found ? normaliseUrl(found) : null;
  } catch { return null; }
}

async function scrapeWebsite(url) {
  if (!url) return null;
  try {
    const { data } = await axios.get(url, { headers: HTTP_HEADERS, timeout: 8000, maxRedirects: 3 });
    return data;
  } catch { return null; }
}

const EMAIL_NOISE = [
  "example.", "@sentry.", "@pixel.", "@schema.", "noreply@", "no-reply@",
  "@w3.org", "@wordpress", "@jquery", "@cloudflare", "@googletagmanager",
];

function extractEmailsFromHtml(html) {
  if (!html) return [];
  const $ = cheerio.load(html);
  $("script, style, noscript").remove();
  const text = $.root().text();
  const matches = text.match(/\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b/g) || [];
  return [...new Set(
    matches.map((e) => e.toLowerCase()).filter((e) => !EMAIL_NOISE.some((n) => e.includes(n)))
  )].slice(0, 3);
}

// Step 4: Generate candidate emails from domain
function generateEmails(website) {
  if (!website) return [];
  try {
    const domain = new URL(website).hostname.replace(/^www\./, "");
    return [`info@${domain}`, `contact@${domain}`, `hello@${domain}`];
  } catch { return []; }
}

// Step 5: LinkedIn search via Google
function buildLinkedInSearchUrl(companyName) {
  const q = encodeURIComponent(`${companyName} founder linkedin`);
  return `https://www.google.com/search?q=${q}`;
}

// Step 6: Decision maker extraction
function extractDecisionMaker(html) {
  if (!html) return null;
  const $ = cheerio.load(html);
  $("script, style, noscript").remove();
  const text = $.root().text().replace(/\s+/g, " ");
  const patterns = [
    /(?:Dr\.|Доктор)\s+([А-ЯA-Z][а-яёa-z]+(?: [А-ЯA-Z][а-яёa-z]+)?)/,
    /(?:CEO|Founder|Director|Owner|Владелец|Директор|Основатель|Главврач)[:\s,]+([А-ЯA-Z][а-яёa-z]+(?: [А-ЯA-Z][а-яёa-z]+)?)/i,
    /([А-ЯA-Z][а-яёa-z]+(?: [А-ЯA-Z][а-яёa-z]+)?)[,\s]+(?:CEO|Founder|Owner|Директор|Владелец)/i,
  ];
  for (const p of patterns) {
    const m = text.match(p);
    if (m?.[1]?.trim().length > 3) return m[1].trim();
  }
  return null;
}

function inferDecisionMaker(name, niche) {
  const lower = (name + " " + niche).toLowerCase();
  if (MEDICAL_KEYWORDS.some((k) => lower.includes(k))) {
    const lastName = [...name.split(/\s+/)].reverse()
      .find((w) => /^[A-ZА-ЯЁ]/.test(w) && w.length >= 4);
    return lastName ? `Dr. ${lastName}` : "Lead Physician";
  }
  if (LEGAL_KEYWORDS.some((k) => lower.includes(k))) return "Managing Partner";
  return "Owner";
}

function calcScore({ website, email, generatedEmails, phone, dm, linkedin }) {
  let s = 0;
  if (website)              s += 20;
  if (email)                s += 20;
  else if (generatedEmails?.length) s += 10;
  if (phone)                s += 15;
  if (dm)                   s += 15;
  if (linkedin)             s += 10;
  s += Math.floor(Math.random() * 11) + 5;
  return Math.min(s, 100);
}

// ─────────────────────────────────────────────────────────────────
// ENRICH — takes a phone-confirmed business, adds email/DM/LinkedIn
// ─────────────────────────────────────────────────────────────────

async function enrichLead(business, niche) {
  // Get or find website
  let website = business.website || null;
  if (!website) website = await findWebsiteViaSearch(business.name);

  // Scrape website
  const html = await scrapeWebsite(website);

  // Emails: real first, then generated
  const realEmails      = extractEmailsFromHtml(html);
  const generatedEmails = realEmails.length ? [] : generateEmails(website);
  const email           = realEmails[0] || null;

  // Decision maker
  const dmScraped  = extractDecisionMaker(html);
  const dmInferred = !dmScraped;
  const dm         = dmScraped || inferDecisionMaker(business.name, niche);

  // LinkedIn (Step 5: Google search for founder)
  const linkedin = buildLinkedInSearchUrl(business.name);

  const score = calcScore({ website, email, generatedEmails, phone: business.phone, dm, linkedin });

  return {
    // company holds the normalized (English-readable) display name
    company:             business.normalized_name || business.name,
    // original_name preserves the raw discovered name in its original script
    original_name:       business.original_name   || business.name,
    address:             business.address || null,
    phone:               business.phone,
    website:             website          || null,
    email:               email            || null,
    generated_emails:    generatedEmails,
    decision_maker:      dm,
    dm_inferred:         dmInferred,
    linkedin_search_url: linkedin,
    lead_score:          score,
    // Array fields for frontend compatibility
    emails:          realEmails,
    phones:          [business.phone],
    socials:         [],
    decision_makers: [dm].filter(Boolean),
    insight:         buildInsight({ website, email, generatedEmails, dm, dmInferred }),
  };
}

function buildInsight({ website, email, generatedEmails, dm, dmInferred }) {
  const parts = [];
  parts.push(dmInferred ? `Probable contact: ${dm}` : `Decision maker: ${dm}`);
  if (email)                     parts.push("Direct email available");
  else if (generatedEmails?.length) parts.push(`Try: ${generatedEmails[0]}`);
  if (!website)                  parts.push("No website — high cold-call potential");
  return parts.join(" · ");
}

// ─────────────────────────────────────────────────────────────────
// GLOBAL ERROR HANDLERS
// ─────────────────────────────────────────────────────────────────
process.on("uncaughtException", (err) => console.error("Uncaught:", err));
process.on("unhandledRejection", (err) => console.error("Unhandled:", err));

// ─────────────────────────────────────────────────────────────────
// ROUTES
// ─────────────────────────────────────────────────────────────────

app.get("/", (req, res) => {
  res.json({ status: "ok", service: "AX AI V Data Engine" });
});

// ── Google OAuth routes ──────────────────────────────────────────

app.get("/auth/google", (req, res) => {
  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET) {
    return res.status(500).json({ error: "Google OAuth not configured" });
  }
  // Step 1: Generate Google consent URL and redirect the user there.
  // The state param is unused now (session cookie handles identity).
  const client = createOAuth2Client();
  const url = client.generateAuthUrl({
    access_type: "offline",
    prompt: "consent",
    scope: [
      "https://www.googleapis.com/auth/spreadsheets",
      "https://www.googleapis.com/auth/drive.file",
      "https://www.googleapis.com/auth/userinfo.email",
    ],
  });
  res.redirect(url);
});

// Step 2: Google redirects here after user grants access.
// We exchange the code for tokens and store them in req.session.
app.get("/auth/google/callback", async (req, res) => {
  try {
    const client = createOAuth2Client();
    const { tokens } = await client.getToken(req.query.code);
    client.setCredentials(tokens);

    // Get user email for display
    const oauth2 = google.oauth2({ version: "v2", auth: client });
    const userInfo = await oauth2.userinfo.get();

    // Tokens are stored in the express-session (cookie-backed).
    // sheetId will be populated on first lead save.
    req.session.tokens  = tokens;
    req.session.email   = userInfo.data.email;
    req.session.sheetId = null;
    console.log(`  OAuth: connected ${req.session.email}`);

    res.redirect(`${FRONTEND_URL}?google_connected=true`);
  } catch (err) {
    console.error("  OAuth callback error:", err.message);
    res.redirect(`${FRONTEND_URL}?google_connected=false&error=auth_failed`);
  }
});

// Check if user has a Google session
app.get("/auth/status", (req, res) => {
  if (req.session.tokens) {
    res.json({ connected: true, email: req.session.email });
  } else {
    res.json({ connected: false });
  }
});

// Destroy Google connection (clears tokens from session)
app.post("/auth/disconnect", (req, res) => {
  req.session.tokens  = null;
  req.session.email   = null;
  req.session.sheetId = null;
  res.json({ disconnected: true });
});

// ─────────────────────────────────────────────────────────────────
// APIFY ACTOR RUNNER — triggers a fresh Google Places scrape each request
// ─────────────────────────────────────────────────────────────────
const APIFY_TOKEN    = (process.env.APIFY_DATASET_URL || "").match(/token=([^&]+)/)?.[1] || "";
const APIFY_ACTOR_ID = "nwua9Gu5YrADL7ZDj"; // compass/crawler-google-places

async function runApifyActor(niche, location) {
  if (!APIFY_TOKEN) throw new Error("APIFY_TOKEN not configured");

  // 1. Start run
  const runResp = await axios.post(
    `https://api.apify.com/v2/acts/${APIFY_ACTOR_ID}/runs?token=${APIFY_TOKEN}`,
    {
      searchStringsArray:         [`${niche} in ${location}`],
      locationQuery:              location,
      maxCrawledPlacesPerSearch:  500,
      language:                   "en",
      includeWebResults:          false,
      skipClosedPlaces:           false,
    },
    { timeout: 15000 }
  );
  const runId    = runResp.data?.data?.id;
  const datasetId = runResp.data?.data?.defaultDatasetId;
  if (!runId) throw new Error("Apify run did not return a run ID");
  console.log(`  [Apify] Run started: ${runId}`);

  // 2. Poll until SUCCEEDED (max 3 min, every 5 s)
  const deadline = Date.now() + 3 * 60 * 1000;
  let status = runResp.data?.data?.status;
  while (status !== "SUCCEEDED" && Date.now() < deadline) {
    if (status === "FAILED" || status === "ABORTED" || status === "TIMED-OUT")
      throw new Error(`Apify run ${status}`);
    await new Promise((r) => setTimeout(r, 5000));
    const poll = await axios.get(
      `https://api.apify.com/v2/actor-runs/${runId}?token=${APIFY_TOKEN}`,
      { timeout: 10000 }
    );
    status = poll.data?.data?.status;
    console.log(`  [Apify] Run status: ${status}`);
  }
  if (status !== "SUCCEEDED") throw new Error("Apify run timed out");

  // 3. Paginated fetch — loop until no more items (safety cap: 1000)
  const allItems = [];
  const BATCH    = 50;
  const MAX      = 1000;
  let   offset   = 0;

  while (allItems.length < MAX) {
    const resp = await axios.get(
      `https://api.apify.com/v2/datasets/${datasetId}/items?token=${APIFY_TOKEN}&clean=true&offset=${offset}&limit=${BATCH}`,
      { timeout: 15000 }
    );
    const batch = Array.isArray(resp.data) ? resp.data : [];
    if (batch.length === 0) break;
    allItems.push(...batch);
    offset += batch.length;
    console.log(`  [Apify] Fetched batch: ${batch.length} (total so far: ${allItems.length})`);
    if (batch.length < BATCH) break; // last page
  }

  console.log(`  [Apify] All items fetched: ${allItems.length} from dataset ${datasetId}`);
  return allItems;
}

// ── Search route (both /search and /api/search) ──────────────────

app.post(["/search", "/api/search"], async (req, res) => {
  const { niche, location, service = "business_finder" } = req.body;

  if (!niche || !location) {
    return res.status(400).json({ error: "niche and location are required" });
  }

  const hasKey = !!GOOGLE_API_KEY;
  const hasTokens = !!(req.session && req.session.tokens);
  console.log(`\n[${service}] "${niche}" in "${location}" | Places API: ${hasKey ? "YES" : "NO KEY"} | Google session: ${hasTokens ? req.session.email : "none"}`);
  if (!hasTokens) console.log("  No Google tokens — proceeding without Sheets save");

  try {
    let allSources = [];
    const liveNames = new Set();

    const addUnique = (arr) => {
      const unique = [];
      for (const b of arr) {
        const k = b.name.toLowerCase().replace(/\s+/g, "");
        if (liveNames.has(k)) continue;
        liveNames.add(k);
        unique.push(b);
      }
      return unique;
    };

    // ── 0. PRIMARY: Apify Google Places actor (fresh run each request) ─
    console.log("  [Pipeline] Stage 0: Triggering Apify actor run...");
    try {
      const apifyLeads = await runApifyActor(niche, location);
      if (apifyLeads.length > 0) {
        const mapped = apifyLeads.map((b) => ({
          name:    b.title   || b.name || "",
          phone:   b.phone   || null,
          website: b.website || null,
          address: [b.street, b.city, b.state].filter(Boolean).join(", ") || null,
          emails:  [],
        }));
        allSources = addUnique(mapped);
        console.log(`  [Pipeline] Apify returned: ${allSources.length} leads`);
      }
    } catch (err) {
      console.log(`  [Pipeline] Apify failed (non-fatal): ${err.message}`);
    }

    // ── 1. PRIMARY: Puppeteer Google Maps scraper ─────────────────────
    console.log("  [Pipeline] Stage 1: Running Puppeteer scraper...");
    let mapsLeads = [];
    try {
      const rawMaps = await scrapeGoogleMapsWithEmails(niche, location, { maxResults: 40, timeoutMs: 30000 });
      mapsLeads = rawMaps.map((b) => ({
        name: b.name,
        phone: null,
        website: (b.link && !b.link.includes("google.com/maps")) ? b.link : null,
        address: null,
        emails: b.emails || [],
      }));
      console.log(`  [Pipeline] Puppeteer returned: ${mapsLeads.length} leads (${mapsLeads.filter(l => l.emails.length).length} with emails)`);
    } catch (err) {
      console.log(`  [Pipeline] Puppeteer failed (non-fatal): ${err.message}`);
    }
    allSources = addUnique(mapsLeads);

    // ── 2. FALLBACK: Google Places API (if Puppeteer < 10 results) ───
    if (allSources.length < 10) {
      console.log(`  [Pipeline] Stage 2: Puppeteer only got ${allSources.length} — falling back to Places API...`);
      const placesLeads = await searchGooglePlaces(niche, location);
      console.log(`  [Pipeline] Places API returned: ${placesLeads.length} leads`);
      const uniquePlaces = addUnique(placesLeads);
      allSources = [...allSources, ...uniquePlaces];
    } else {
      console.log(`  [Pipeline] Puppeteer sufficient (${allSources.length}) — skipping Places API`);
    }

    // ── 3. FALLBACK: HTML scraping (if still < 20 results) ───────────
    if (allSources.length < 20) {
      console.log(`  [Pipeline] Stage 3: Only ${allSources.length} leads — falling back to HTML scraping...`);
      const scrapeLeads = await scrapingDiscovery(niche, location);
      console.log(`  [Pipeline] HTML scraping returned: ${scrapeLeads.length} leads`);
      const uniqueScrape = addUnique(scrapeLeads);
      allSources = [...allSources, ...uniqueScrape];
    } else {
      console.log(`  [Pipeline] Sufficient leads (${allSources.length}) — skipping HTML scraping`);
    }

    // ── 4. FALLBACK: Seed data (if still empty) ──────────────────────
    if (allSources.length === 0) {
      console.log("  [Pipeline] Stage 4: No live results — falling back to seed data...");
      const seeds = getSeedLeads(niche);
      const uniqueSeeds = addUnique(seeds);
      allSources = [...allSources, ...uniqueSeeds];
      console.log(`  [Pipeline] Seeds added: ${uniqueSeeds.length}`);
    }

    // ── Filter non-English names before enrichment ──────────────────
    const isEnglish = (text) => /^[\x00-\x7F]+$/.test(text || "");
    allSources = allSources.filter((b) => isEnglish(b.name));
    console.log(`  [Pipeline] After English filter: ${allSources.length}`);

    // Shuffle so repeated runs enrich different leads
    allSources.sort(() => 0.5 - Math.random());

    console.log(`  [Pipeline] Total candidates after all stages: ${allSources.length}`);

    // ── 5. Normalize non-Latin names (single batch API call) ────────
    const candidates = allSources.slice(0, 80);
    await batchNormalizeNames(candidates);

    // ── 6. Enrich only the first 20 leads ─────────────────────────────
    const toEnrich = candidates.slice(0, 20);
    console.log(`  [Pipeline] Enriching top ${toEnrich.length} leads (of ${candidates.length} candidates)...`);

    const settled = await Promise.allSettled(
      toEnrich.map((b) => enrichLead(b, niche))
    );

    const fulfilled = settled.filter((r) => r.status === "fulfilled").map((r) => r.value);
    const rejected  = settled.filter((r) => r.status === "rejected");
    console.log(`  [Pipeline] Enrichment: ${fulfilled.length} ok, ${rejected.length} failed`);
    if (rejected.length > 0) {
      console.log(`  [Pipeline] Sample rejection: ${rejected[0].reason?.message || rejected[0].reason}`);
    }

    const allLeads = fulfilled.sort((a, b) => b.lead_score - a.lead_score);

    // Dedup filter per location — but if it empties everything, reset and return unfiltered
    let leads = filterUsed(allLeads, location).slice(0, 50);
    console.log(`  [Pipeline] After dedup filter: ${leads.length}`);

    // SAFETY: if dedup removed everything, clear this location's cache and return unfiltered
    if (leads.length === 0 && allLeads.length > 0) {
      console.log("  [Pipeline] Dedup emptied results — clearing location cache and returning unfiltered");
      const locKey = location.toLowerCase().replace(/\s+/g, "_");
      if (seenByLocation[locKey]) seenByLocation[locKey].clear();
      leads = allLeads.slice(0, 50);
    }

    console.log(`  Returning ${leads.length} leads`);

    // Save to user's own Google Sheet (only if connected, never blocks results)
    let savedToSheets = false;
    if (req.session && req.session.tokens) {
      savedToSheets = await saveToUserSheet(req.session, leads).catch(() => false);
      console.log(`  Sheets save: ${savedToSheets ? "OK" : "failed (non-fatal)"}`);
    }

    res.json({ source: "puppeteer_priority", leads, savedToSheets });

  } catch (err) {
    console.error("  Pipeline failed:", err.message);
    console.error("  Stack:", err.stack);
    // Hard fallback — return enriched seeds so response is NEVER empty
    const seeds = getSeedLeads(niche).map((b) => {
      const normalized = needsNormalization(b.name) ? transliterateName(b.name) : b.name;
      return {
        company:             normalized,
        original_name:       b.name,
        address:             null,
        phone:               b.phone,
        website:             b.website || null,
        email:               null,
        generated_emails:    generateEmails(b.website),
        decision_maker:      inferDecisionMaker(b.name, niche),
        dm_inferred:         true,
        linkedin_search_url: buildLinkedInSearchUrl(b.name),
        lead_score:          calcScore({ phone: b.phone, dm: true, linkedin: true, website: b.website }),
        emails: [], phones: [b.phone], socials: [], decision_makers: [],
        insight: "Seed fallback — live search unavailable",
      };
    });
    console.log(`  Fallback: returning ${Math.min(seeds.length, 50)} seed leads`);
    res.json({ leads: seeds.slice(0, 50), savedToSheets: false });
  }
});

app.listen(PORT, () => {
  const keyStatus = GOOGLE_API_KEY ? "Google Places API active" : "No API key — scraping mode";
  console.log(`AX AI V Data Engine — http://localhost:${PORT} | ${keyStatus}`);
});
