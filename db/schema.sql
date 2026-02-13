PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- ============================================================
-- TRIBUNALS
-- ============================================================
CREATE TABLE IF NOT EXISTS tribunals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    slug            TEXT NOT NULL UNIQUE,
    region          TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_tribunals_slug ON tribunals(slug);
CREATE INDEX IF NOT EXISTS idx_tribunals_region ON tribunals(region);

-- ============================================================
-- LISTINGS (upcoming and historical auctions)
-- ============================================================
CREATE TABLE IF NOT EXISTS listings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    licitor_id          INTEGER NOT NULL UNIQUE,
    url_path            TEXT NOT NULL,

    -- Status
    status              TEXT NOT NULL DEFAULT 'upcoming'
                        CHECK(status IN ('upcoming', 'past', 'cancelled')),
    is_historical       INTEGER NOT NULL DEFAULT 0,

    -- Property info
    property_type       TEXT,
    description         TEXT,
    surface_m2          REAL,
    energy_rating       TEXT,
    occupancy_status    TEXT,

    -- Location
    department_code     TEXT,
    city                TEXT,
    full_address        TEXT,
    latitude            REAL,
    longitude           REAL,
    cadastral_ref       TEXT,

    -- Auction info
    tribunal_id         INTEGER REFERENCES tribunals(id),
    auction_date        TEXT,
    auction_time        TEXT,
    mise_a_prix         INTEGER,
    case_reference      TEXT,

    -- Legal representatives
    lawyer_name         TEXT,
    lawyer_phone        TEXT,

    -- Visit info
    visit_date          TEXT,

    -- Price context (regional data from detail page)
    price_per_m2_min    REAL,
    price_per_m2_avg    REAL,
    price_per_m2_max    REAL,

    -- Engagement metrics
    view_count          INTEGER,
    favorites_count     INTEGER,

    -- Scraping metadata
    publication_date    TEXT,
    first_scraped_at    TEXT DEFAULT (datetime('now')),
    last_scraped_at     TEXT DEFAULT (datetime('now')),
    detail_scraped      INTEGER NOT NULL DEFAULT 0,

    has_price_reduction TEXT,

    -- Adjudication result (from results pages)
    result_status       TEXT CHECK(result_status IN ('sold', 'carence', 'non_requise')),
    final_price         INTEGER,
    result_date         TEXT
);

CREATE INDEX IF NOT EXISTS idx_listings_licitor_id ON listings(licitor_id);
CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status);
CREATE INDEX IF NOT EXISTS idx_listings_auction_date ON listings(auction_date);
CREATE INDEX IF NOT EXISTS idx_listings_department ON listings(department_code);
CREATE INDEX IF NOT EXISTS idx_listings_tribunal ON listings(tribunal_id);
CREATE INDEX IF NOT EXISTS idx_listings_mise_a_prix ON listings(mise_a_prix);
CREATE INDEX IF NOT EXISTS idx_listings_property_type ON listings(property_type);
CREATE INDEX IF NOT EXISTS idx_listings_coords ON listings(latitude, longitude)
    WHERE latitude IS NOT NULL;

-- ============================================================
-- ADJUDICATION RESULTS (manual entry or external source)
-- ============================================================
CREATE TABLE IF NOT EXISTS adjudication_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id      INTEGER NOT NULL REFERENCES listings(id),
    final_price     INTEGER,
    price_source    TEXT NOT NULL DEFAULT 'manual'
                    CHECK(price_source IN ('manual', 'external', 'estimated')),
    buyer_type      TEXT,
    notes           TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(listing_id)
);

CREATE INDEX IF NOT EXISTS idx_adjudication_listing ON adjudication_results(listing_id);

-- ============================================================
-- ALERTS
-- ============================================================
CREATE TABLE IF NOT EXISTS alerts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    is_active       INTEGER NOT NULL DEFAULT 1,

    -- Criteria (NULL = no filter on this dimension)
    min_price       INTEGER,
    max_price       INTEGER,
    department_codes TEXT,
    regions         TEXT,
    property_types  TEXT,
    min_surface     REAL,
    max_surface     REAL,
    tribunal_slugs  TEXT,

    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

-- ============================================================
-- ALERT MATCHES
-- ============================================================
CREATE TABLE IF NOT EXISTS alert_matches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id        INTEGER NOT NULL REFERENCES alerts(id) ON DELETE CASCADE,
    listing_id      INTEGER NOT NULL REFERENCES listings(id),
    matched_at      TEXT DEFAULT (datetime('now')),
    is_seen         INTEGER NOT NULL DEFAULT 0,
    UNIQUE(alert_id, listing_id)
);

CREATE INDEX IF NOT EXISTS idx_alert_matches_alert ON alert_matches(alert_id);
CREATE INDEX IF NOT EXISTS idx_alert_matches_unseen
    ON alert_matches(is_seen) WHERE is_seen = 0;

-- ============================================================
-- SCRAPE LOG
-- ============================================================
CREATE TABLE IF NOT EXISTS scrape_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT DEFAULT (datetime('now')),
    finished_at     TEXT,
    scrape_type     TEXT NOT NULL CHECK(scrape_type IN (
                        'full_index', 'incremental', 'history',
                        'detail_backfill', 'map_backfill', 'surface_backfill'
                    )),
    pages_scraped   INTEGER DEFAULT 0,
    listings_new    INTEGER DEFAULT 0,
    listings_updated INTEGER DEFAULT 0,
    errors          INTEGER DEFAULT 0,
    notes           TEXT
);
