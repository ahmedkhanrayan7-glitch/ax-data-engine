-- ─────────────────────────────────────────────────────────────────────────────
-- AX Decision Intelligence Engine — Database Schema
-- PostgreSQL 16
-- ─────────────────────────────────────────────────────────────────────────────

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";     -- Trigram indexes for fuzzy search
CREATE EXTENSION IF NOT EXISTS "unaccent";    -- Accent-insensitive search

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. API KEYS & TENANTS
-- Multi-tenant SaaS: each customer has an API key tied to a tenant
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE tenants (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(200) NOT NULL,
    email           VARCHAR(200) UNIQUE NOT NULL,
    plan            VARCHAR(50) DEFAULT 'starter',    -- starter | pro | enterprise
    quota_monthly   INTEGER DEFAULT 1000,              -- searches/month
    quota_used      INTEGER DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    is_active       BOOLEAN DEFAULT TRUE
);

CREATE TABLE api_keys (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    key_hash        VARCHAR(64) UNIQUE NOT NULL,    -- SHA-256 of actual key
    key_prefix      VARCHAR(10) NOT NULL,           -- First 8 chars for display
    name            VARCHAR(100),                   -- Human label e.g. "Production"
    scopes          TEXT[] DEFAULT '{"search","export"}',
    last_used_at    TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    is_active       BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX idx_api_keys_tenant ON api_keys(tenant_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. SEARCH JOBS
-- Tracks every search request from submission to completion
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE search_jobs (
    id              UUID PRIMARY KEY,               -- Same as Celery task_id
    tenant_id       UUID NOT NULL REFERENCES tenants(id),
    api_key_id      UUID REFERENCES api_keys(id),

    -- Request parameters
    niche           VARCHAR(200) NOT NULL,
    location        VARCHAR(300) NOT NULL,
    roles           TEXT[] NOT NULL,
    depth           VARCHAR(20) NOT NULL,
    max_results     INTEGER NOT NULL,
    language_hint   VARCHAR(10),
    webhook_url     TEXT,

    -- Status tracking
    status          VARCHAR(20) DEFAULT 'pending',  -- pending|running|completed|failed
    progress        SMALLINT DEFAULT 0,
    total_found     INTEGER DEFAULT 0,
    processed       INTEGER DEFAULT 0,

    -- Timing
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    processing_time_ms INTEGER,

    -- Error tracking
    error_message   TEXT,
    retry_count     SMALLINT DEFAULT 0
);

CREATE INDEX idx_jobs_tenant ON search_jobs(tenant_id);
CREATE INDEX idx_jobs_status ON search_jobs(status);
CREATE INDEX idx_jobs_created ON search_jobs(created_at DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. COMPANIES (Business entities)
-- Normalized business records — deduplicated across searches
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE companies (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(300) NOT NULL,
    normalized_name VARCHAR(300),                   -- Lowercase, no punctuation
    website         VARCHAR(500),
    domain          VARCHAR(200),                   -- Extracted domain only
    phone           VARCHAR(50),
    address         TEXT,
    city            VARCHAR(200),
    country         VARCHAR(100),
    google_maps_url TEXT,
    google_rating   NUMERIC(2,1),
    review_count    INTEGER,
    categories      TEXT[],

    -- Enrichment
    company_size    VARCHAR(20),
    employee_count  INTEGER,
    revenue_estimate VARCHAR(50),
    year_founded    SMALLINT,
    tech_stack      TEXT[],
    social_presence JSONB DEFAULT '{}',
    has_paid_ads    BOOLEAN DEFAULT FALSE,

    -- Metadata
    data_sources    TEXT[],
    first_seen_at   TIMESTAMPTZ DEFAULT NOW(),
    last_updated_at TIMESTAMPTZ DEFAULT NOW(),
    crawl_status    VARCHAR(20) DEFAULT 'pending'
);

-- Fast lookup by domain (most common dedup key)
CREATE UNIQUE INDEX idx_companies_domain ON companies(domain) WHERE domain IS NOT NULL;
CREATE INDEX idx_companies_name_trgm ON companies USING gin(normalized_name gin_trgm_ops);
CREATE INDEX idx_companies_city ON companies(city);
CREATE INDEX idx_companies_country ON companies(country);

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. DECISION MAKERS
-- Individual people associated with companies
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE decision_makers (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id      UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,

    name            VARCHAR(200) NOT NULL,
    normalized_name VARCHAR(200),
    role            VARCHAR(100),
    confidence_score SMALLINT CHECK (confidence_score BETWEEN 0 AND 100),
    source          VARCHAR(100),               -- nlp_ner|regex_pattern|schema_org|niche_heuristic
    verified        BOOLEAN DEFAULT FALSE,
    profile_url     TEXT,                       -- LinkedIn URL if found

    first_seen_at   TIMESTAMPTZ DEFAULT NOW(),
    last_confirmed_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_dm_company ON decision_makers(company_id);
CREATE INDEX idx_dm_name_trgm ON decision_makers USING gin(normalized_name gin_trgm_ops);
CREATE INDEX idx_dm_role ON decision_makers(role);
CREATE INDEX idx_dm_confidence ON decision_makers(confidence_score DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. CONTACT RECORDS
-- Email addresses and phone numbers, with validation status
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE contact_emails (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id      UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    decision_maker_id UUID REFERENCES decision_makers(id),

    email           VARCHAR(254) NOT NULL,
    domain          VARCHAR(200),
    email_type      VARCHAR(20) DEFAULT 'unknown',  -- personal|role_based|generic
    status          VARCHAR(20) DEFAULT 'unknown',  -- valid|catch_all|invalid|risky|unknown
    pattern_used    VARCHAR(50),                    -- Which generation pattern matched

    last_validated_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_emails_company ON contact_emails(company_id);
CREATE INDEX idx_emails_email ON contact_emails(email);
CREATE INDEX idx_emails_status ON contact_emails(status);

CREATE TABLE contact_phones (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id      UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,

    phone_e164      VARCHAR(20) NOT NULL,        -- +79991234567
    phone_national  VARCHAR(30),                 -- (999) 123-45-67
    country_code    VARCHAR(5),
    verified        BOOLEAN DEFAULT FALSE,

    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_phones_company ON contact_phones(company_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- 6. OPPORTUNITY SIGNALS
-- Per-company opportunity analysis results
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE opportunity_signals (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    company_id      UUID NOT NULL REFERENCES companies(id) ON DELETE CASCADE,

    signal          TEXT NOT NULL,
    category        VARCHAR(50),                -- hiring|reviews|missing_system|growth|pain_point
    severity        VARCHAR(20) DEFAULT 'medium',
    detail          TEXT,

    detected_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_signals_company ON opportunity_signals(company_id);
CREATE INDEX idx_signals_category ON opportunity_signals(category);
CREATE INDEX idx_signals_severity ON opportunity_signals(severity);

-- ─────────────────────────────────────────────────────────────────────────────
-- 7. LEAD RESULTS
-- Links a job, a company, and its computed lead score
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE lead_results (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id          UUID NOT NULL REFERENCES search_jobs(id) ON DELETE CASCADE,
    company_id      UUID NOT NULL REFERENCES companies(id),

    lead_score      SMALLINT CHECK (lead_score BETWEEN 0 AND 100),
    score_breakdown JSONB DEFAULT '{}',
    rank            INTEGER,                     -- Position in job results

    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_lead_results_job ON lead_results(job_id);
CREATE INDEX idx_lead_results_company ON lead_results(company_id);
CREATE INDEX idx_lead_results_score ON lead_results(lead_score DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- 8. USAGE METRICS (for billing)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE usage_events (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       UUID NOT NULL REFERENCES tenants(id),
    job_id          UUID REFERENCES search_jobs(id),
    event_type      VARCHAR(50) NOT NULL,        -- search|export|api_call
    credits_used    INTEGER DEFAULT 1,
    metadata        JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_usage_tenant_date ON usage_events(tenant_id, created_at DESC);

-- ─────────────────────────────────────────────────────────────────────────────
-- TRIGGERS: updated_at auto-maintenance
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_tenants_updated_at
    BEFORE UPDATE ON tenants
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_companies_updated_at
    BEFORE UPDATE ON companies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ─────────────────────────────────────────────────────────────────────────────
-- USEFUL VIEWS
-- ─────────────────────────────────────────────────────────────────────────────

-- Full lead view (joins company + DMs + emails + signals)
CREATE VIEW v_enriched_leads AS
SELECT
    c.id                            AS company_id,
    c.name                          AS company_name,
    c.website,
    c.domain,
    c.address,
    c.google_rating,
    c.review_count,
    c.company_size,
    c.revenue_estimate,
    c.tech_stack,
    c.categories,
    COALESCE(
        json_agg(DISTINCT jsonb_build_object(
            'name', dm.name,
            'role', dm.role,
            'confidence', dm.confidence_score,
            'source', dm.source
        )) FILTER (WHERE dm.id IS NOT NULL),
        '[]'::json
    )                               AS decision_makers,
    COALESCE(
        array_agg(DISTINCT ce.email) FILTER (WHERE ce.status IN ('valid','catch_all')),
        '{}'::text[]
    )                               AS verified_emails,
    COALESCE(
        array_agg(DISTINCT cp.phone_e164) FILTER (WHERE cp.id IS NOT NULL),
        '{}'::text[]
    )                               AS phones,
    COALESCE(
        json_agg(DISTINCT jsonb_build_object(
            'signal', os.signal,
            'category', os.category,
            'severity', os.severity
        )) FILTER (WHERE os.id IS NOT NULL),
        '[]'::json
    )                               AS opportunity_signals
FROM companies c
LEFT JOIN decision_makers dm ON dm.company_id = c.id
LEFT JOIN contact_emails ce ON ce.company_id = c.id
LEFT JOIN contact_phones cp ON cp.company_id = c.id
LEFT JOIN opportunity_signals os ON os.company_id = c.id
GROUP BY c.id;

-- Daily usage summary per tenant
CREATE VIEW v_daily_usage AS
SELECT
    tenant_id,
    DATE(created_at)                AS date,
    COUNT(*)                        AS events,
    SUM(credits_used)               AS credits
FROM usage_events
GROUP BY tenant_id, DATE(created_at);
