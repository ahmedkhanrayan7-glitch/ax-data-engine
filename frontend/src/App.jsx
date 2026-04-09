import { useState } from "react";
import axios from "axios";
import "./App.css";

const API_URL = import.meta.env.VITE_API_URL || "";

const SERVICES = [
  {
    id: "business_finder",
    icon: "⬡",
    title: "Business Finder",
    description: "Discover businesses by niche and location",
  },
  {
    id: "decision_makers",
    icon: "◈",
    title: "Decision Makers",
    description: "Find owners, directors, and key contacts",
  },
  {
    id: "email_extractor",
    icon: "◎",
    title: "Email Extractor",
    description: "Extract verified email addresses",
  },
  {
    id: "reviews_analyzer",
    icon: "◇",
    title: "Reviews Analyzer",
    description: "Analyze ratings and review insights",
  },
  {
    id: "company_intel",
    icon: "▣",
    title: "Company Intel",
    description: "Full intelligence profile per business",
  },
];

const ROLE_OPTIONS = [
  "CEO", "CFO", "Founder", "Co-Founder", "Owner", "Director", "Manager",
];

const SCORE_COLOR = (s) => {
  if (s >= 75) return "score-high";
  if (s >= 50) return "score-mid";
  return "score-low";
};

export default function App() {
  const [consent, setConsent] = useState(() => localStorage.getItem("user_consent") === "true");
  const [activeService, setActiveService] = useState("business_finder");
  const [niche,    setNiche]    = useState("");
  const [location, setLocation] = useState("");
  const [roles,    setRoles]    = useState([]);
  const [results,  setResults]  = useState([]);
  const [loading,  setLoading]  = useState(false);
  const [error,    setError]    = useState("");
  const [searched, setSearched] = useState(false);
  const [savedMsg, setSavedMsg] = useState("");

  function acceptConsent() {
    localStorage.setItem("user_consent", "true");
    setConsent(true);
  }

  function toggleRole(role) {
    setRoles((prev) =>
      prev.includes(role) ? prev.filter((r) => r !== role) : [...prev, role]
    );
  }

  async function handleSearch(e) {
    e.preventDefault();
    if (!niche.trim() || !location.trim()) return;

    setLoading(true);
    setError("");
    setResults([]);
    setSearched(false);

    try {
      const res = await axios.post(`${API_URL}/api/search`, {
        service: activeService,
        niche: niche.trim(),
        location: location.trim(),
        roles,
      });
      setResults(res.data);
      setSearched(true);
      if (res.data.length > 0) {
        setSavedMsg("Leads saved to Google Sheets");
        setTimeout(() => setSavedMsg(""), 4000);
      }
    } catch {
      setError("Could not reach the server. Please try again later.");
    } finally {
      setLoading(false);
    }
  }

  const service = SERVICES.find((s) => s.id === activeService);

  if (!consent) {
    return (
      <div className="consent-overlay">
        <div className="consent-modal">
          <span className="consent-icon">▲</span>
          <h2 className="consent-title">Privacy & Data Consent</h2>
          <p className="consent-text">
            By using this platform, you agree that all extracted business data
            (emails, phone numbers, contacts) will be stored in Google Sheets.
          </p>
          <button className="consent-btn" onClick={acceptConsent}>
            I Agree — Continue
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="app">

      {savedMsg && <div className="saved-banner">{savedMsg}</div>}

      {/* ── Header ── */}
      <header className="header">
        <div className="header-inner">
          <div className="brand">
            <span className="brand-mark">▲</span>
            <span className="brand-name">AX <span className="brand-accent">AI V</span> Data Engine</span>
          </div>
          <span className="header-tag">Lead Intelligence Platform</span>
        </div>
      </header>

      <main className="main">

        {/* ── Service Selector ── */}
        <section className="services-section">
          <p className="section-label">SELECT SERVICE</p>
          <div className="services-grid">
            {SERVICES.map((s) => (
              <button
                key={s.id}
                className={`service-card ${activeService === s.id ? "active" : ""}`}
                onClick={() => { setActiveService(s.id); setResults([]); setSearched(false); }}
              >
                <span className="service-icon">{s.icon}</span>
                <span className="service-title">{s.title}</span>
                <span className="service-desc">{s.description}</span>
              </button>
            ))}
          </div>
        </section>

        {/* ── Search Form ── */}
        <section className="form-section">
          <form className="search-form" onSubmit={handleSearch}>
            <div className="form-row">
              <div className="field">
                <label>Business Niche</label>
                <input
                  value={niche}
                  onChange={(e) => setNiche(e.target.value)}
                  placeholder="e.g. dental clinic, gym, restaurant"
                  required
                />
              </div>
              <div className="field">
                <label>Location</label>
                <input
                  value={location}
                  onChange={(e) => setLocation(e.target.value)}
                  placeholder="e.g. Moscow, London, New York"
                  required
                />
              </div>
            </div>

            {(activeService === "decision_makers" || activeService === "company_intel") && (
              <div className="roles-field">
                <label>Filter by Roles <span className="optional">(optional)</span></label>
                <div className="roles-chips">
                  {ROLE_OPTIONS.map((r) => (
                    <button
                      key={r}
                      type="button"
                      className={`chip ${roles.includes(r) ? "chip-on" : ""}`}
                      onClick={() => toggleRole(r)}
                    >
                      {r}
                    </button>
                  ))}
                </div>
              </div>
            )}

            <button type="submit" className="run-btn" disabled={loading}>
              {loading
                ? <><span className="spinner" /> Extracting data…</>
                : `Run ${service.title}`}
            </button>
          </form>
        </section>

        {error && <p className="error-banner">{error}</p>}

        {/* ── Results ── */}
        {searched && !loading && (
          <section className="results-section">
            <div className="results-header">
              <span className="results-count">
                {results.length > 0
                  ? `${results.length} results · sorted by lead score`
                  : "No results found"}
              </span>
              {results.length > 0 && (
                <span className="service-badge">{service.title}</span>
              )}
            </div>

            {results.length > 0 && (
              <div className="results-grid">
                {results.map((item, i) => (
                  <ResultCard key={i} item={item} service={activeService} rank={i + 1} />
                ))}
              </div>
            )}
          </section>
        )}
      </main>
    </div>
  );
}

function ResultCard({ item, service, rank }) {
  return (
    <div className="result-card">
      <div className="rc-top">
        <div className="rc-rank">#{rank}</div>
        <div className="rc-company">
          <span className="rc-name">{item.company}</span>
          {item.original_name && item.original_name !== item.company && (
            <span className="rc-original-name">{item.original_name}</span>
          )}
          {item.address && <span className="rc-address">{item.address}</span>}
        </div>
        <div className={`rc-score ${SCORE_COLOR(item.lead_score)}`}>
          <span className="score-num">{item.lead_score}</span>
          <span className="score-label">score</span>
        </div>
      </div>

      {item.insight && (
        <div className="rc-insight">{item.insight}</div>
      )}

      <div className="rc-data">
        {item.website && (
          <DataRow label="Website">
            <a href={item.website} target="_blank" rel="noreferrer" className="link">
              {item.website.replace(/https?:\/\//, "")}
            </a>
          </DataRow>
        )}

        {item.decision_makers?.length > 0 && (
          <DataRow label="Decision Makers">
            <div className="tag-list">
              {item.decision_makers.map((dm, i) => (
                <span key={i} className={`tag ${item.dm_inferred ? "tag-inferred" : "tag-blue"}`}>
                  {dm}{item.dm_inferred && <span className="inferred-mark"> ~</span>}
                </span>
              ))}
            </div>
          </DataRow>
        )}

        {item.emails?.length > 0 && (
          <DataRow label="Emails">
            <div className="tag-list">
              {item.emails.map((e, i) => (
                <span key={i} className="tag tag-green">{e}</span>
              ))}
            </div>
          </DataRow>
        )}

        {!item.emails?.length && item.generated_email && (
          <DataRow label="Suggested Email">
            <span className="tag tag-orange" title="Generated — not scraped directly">
              {item.generated_email} <span className="inferred-mark">~</span>
            </span>
          </DataRow>
        )}

        {item.phones?.length > 0 && (
          <DataRow label="Phones">
            <div className="tag-list">
              {item.phones.map((p, i) => (
                <span key={i} className="tag tag-purple">{p}</span>
              ))}
            </div>
          </DataRow>
        )}

        {item.linkedin_search_url && (
          <DataRow label="LinkedIn">
            <a href={item.linkedin_search_url} target="_blank" rel="noreferrer" className="tag tag-linkedin">
              Search on LinkedIn ↗
            </a>
          </DataRow>
        )}

        {service === "reviews_analyzer" && item.insight?.includes("Rating") && (
          <DataRow label="Reviews">
            <span className="tag tag-orange">{item.insight.split("·")[0].trim()}</span>
          </DataRow>
        )}

        {item.socials?.length > 0 && (
          <DataRow label="Socials">
            <div className="tag-list">
              {item.socials.slice(0, 3).map((s, i) => {
                const platform = s.includes("instagram") ? "Instagram"
                  : s.includes("vk.com") ? "VK"
                  : s.includes("t.me") || s.includes("telegram") ? "Telegram"
                  : s.includes("facebook") ? "Facebook"
                  : "Social";
                return (
                  <a key={i} href={s} target="_blank" rel="noreferrer" className="tag tag-gray">
                    {platform}
                  </a>
                );
              })}
            </div>
          </DataRow>
        )}
      </div>
    </div>
  );
}

function DataRow({ label, children }) {
  return (
    <div className="data-row">
      <span className="data-label">{label}</span>
      <div className="data-value">{children}</div>
    </div>
  );
}
