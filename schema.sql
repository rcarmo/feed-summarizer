-- ============================================================================
-- Feed Summarizer Database Schema
-- ---------------------------------------------------------------------------
-- This schema stores:
--   * Feed metadata + HTTP cache headers + transient error tracking
--   * Raw fetched items (1 row per GUID / URL) – immutable once inserted
--   * AI summaries (1:1 with items) with generation & publication timestamps
--   * Bulletins (grouped publishing sessions) and their summary membership
--
-- Conventions:
--   * INTEGER timestamps are Unix epoch seconds (UTC)
--   * Nullable columns left NULL when value not yet known (e.g. published_date)
--   * Text columns storing JSON (e.g. feed_slugs) are minified arrays
--   * Foreign keys defined for referential clarity (SQLite enforcement depends
--     on PRAGMA foreign_keys=ON at runtime)
--   * Error tracking is reset (error_count -> 0, last_error -> NULL) on the
--     next successful fetch.
--
-- ============================================================================
PRAGMA journal_mode=WAL; -- Enable WAL mode for better concurrency (many readers + writer)

-- ---------------------------------------------------------------------------
-- feeds
-- One row per configured source. HTTP cache headers & transient failure state
-- are stored here to support conditional requests + exponential backoff.
-- Columns:
--   id            PK
--   slug          Stable identifier used in config & grouping (unique)
--   title         Optional human-readable title discovered from feed
--   url           Canonical feed URL (RSS/Atom or API endpoint)
--   last_fetched  Epoch seconds when successfully (or unsuccessfully) attempted
--   etag          Last received ETag header (opaque)
--   last_modified Last received Last-Modified header (verbatim string)
--   error_count   Consecutive failed attempts (resets on success)
--   last_error    Last error message / status detail (NULL on success/reset)
CREATE TABLE IF NOT EXISTS feeds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT UNIQUE NOT NULL,
    title TEXT,
    url TEXT NOT NULL,
    last_fetched INTEGER,           -- UTC epoch seconds of last attempt
    etag TEXT,                      -- HTTP ETag from previous successful 200/304
    last_modified TEXT,             -- HTTP Last-Modified header value
    error_count INTEGER DEFAULT 0,  -- Consecutive fetch failures (for backoff)
    last_error TEXT                 -- Diagnostic string for last failure
);

-- ---------------------------------------------------------------------------
-- items
-- Raw normalized entries from feeds. An item is inserted only once per GUID/URL.
-- Columns:
--   id       PK; also the FK used by summaries.id (1:1 mapping)
--   feed_id  FK -> feeds.id
--   title    Title extracted from feed item
--   url      Canonical link (UNIQUE to prevent duplicates)
--   guid     Feed-provided GUID or synthesized stable identifier
--   body     Raw or reader-mode fetched content (may include HTML/Markdown)
--   date     Original publication date (epoch seconds, may be NULL if absent)
CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    feed_id INTEGER NOT NULL,
    title TEXT,
    url TEXT UNIQUE,
    guid TEXT,
    body TEXT,
    date INTEGER,                  -- Original published time (UTC epoch) if known
    FOREIGN KEY (feed_id) REFERENCES feeds(id)
);

-- ---------------------------------------------------------------------------
-- summaries
-- AI-generated summaries mapped 1:1 with items via shared primary key.
-- Columns:
--   id             PK & FK -> items.id (exists only after summarization)
--   summary_text   Generated summary content (Markdown / text)
--   topic          AI-assigned topic / category label
--   generated_date Epoch seconds when summarization completed
--   published_date Epoch seconds when first published (HTML/RSS). NULL until published.
CREATE TABLE IF NOT EXISTS summaries (
    id INTEGER PRIMARY KEY,
    summary_text TEXT,           -- AI summary (may be NULL if generation failed)
    topic TEXT,                  -- Classification label
    generated_date INTEGER,      -- UTC epoch seconds summary created
    published_date INTEGER,      -- UTC epoch seconds first published (NULL = pending)
    FOREIGN KEY (id) REFERENCES items(id)
);

-- ---------------------------------------------------------------------------
-- bulletins
-- A bulletin groups a set of summaries for a group_name within a publishing
-- session (time slice). Large sessions may be chunked (session_key suffixes).
-- Columns:
--   id            PK
--   group_name    Name of summary group (maps to summaries grouping config)
--   session_key   Time-slice identifier (YYYY-MM-DD-HH-MM[-chunk])
--   created_date  Epoch seconds bulletin persisted
--   title         AI or fallback title string
--   introduction  Optional AI-generated intro paragraph
--   summary_count Cached number of associated summaries for faster listing
--   feed_slugs    JSON array of source feed slugs participating
CREATE TABLE IF NOT EXISTS bulletins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    session_key TEXT NOT NULL,
    created_date INTEGER NOT NULL,
    title TEXT,
    introduction TEXT,
    summary_count INTEGER DEFAULT 0,
    feed_slugs TEXT,
    UNIQUE(group_name, session_key)
);

-- ---------------------------------------------------------------------------
-- bulletin_summaries
-- Join table linking bulletins to their constituent summaries (many-to-many).
-- Deleting a bulletin or summary cascades to remove linkage rows.
CREATE TABLE IF NOT EXISTS bulletin_summaries (
    bulletin_id INTEGER NOT NULL,
    summary_id INTEGER NOT NULL,
    PRIMARY KEY (bulletin_id, summary_id),
    FOREIGN KEY (bulletin_id) REFERENCES bulletins(id) ON DELETE CASCADE,
    FOREIGN KEY (summary_id) REFERENCES summaries(id) ON DELETE CASCADE
);

-- ---------------------------------------------------------------------------
-- Performance Indexes
-- Focused on common lookups: feed fetch cycles, unpublished summaries, bulletin
-- assembly, and pruning/expiration scans.
-- ---------------------------------------------------------------------------

-- Feeds table indexes
CREATE INDEX IF NOT EXISTS idx_feeds_slug ON feeds(slug);
CREATE INDEX IF NOT EXISTS idx_feeds_last_fetched ON feeds(last_fetched);

-- Items table indexes  
CREATE INDEX IF NOT EXISTS idx_items_feed_id ON items(feed_id);
CREATE INDEX IF NOT EXISTS idx_items_url ON items(url);
CREATE INDEX IF NOT EXISTS idx_items_guid ON items(guid);
CREATE INDEX IF NOT EXISTS idx_items_date ON items(date);
CREATE INDEX IF NOT EXISTS idx_items_feed_guid ON items(feed_id, guid);
CREATE INDEX IF NOT EXISTS idx_items_feed_date ON items(feed_id, date DESC);

-- Summaries table indexes (for topic-based queries)
CREATE INDEX IF NOT EXISTS idx_summaries_topic ON summaries(topic);
CREATE INDEX IF NOT EXISTS idx_summaries_generated_date ON summaries(generated_date);
CREATE INDEX IF NOT EXISTS idx_summaries_published_date ON summaries(published_date);
CREATE INDEX IF NOT EXISTS idx_summaries_topic_published ON summaries(topic, published_date);
CREATE INDEX IF NOT EXISTS idx_summaries_published_not_null ON summaries(published_date) WHERE published_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_summaries_published_null ON summaries(id) WHERE published_date IS NULL;

-- Bulletins table indexes
CREATE INDEX IF NOT EXISTS idx_bulletins_group_name ON bulletins(group_name);
CREATE INDEX IF NOT EXISTS idx_bulletins_session_key ON bulletins(session_key);
CREATE INDEX IF NOT EXISTS idx_bulletins_created_date ON bulletins(created_date);
CREATE INDEX IF NOT EXISTS idx_bulletins_group_session ON bulletins(group_name, session_key);
CREATE INDEX IF NOT EXISTS idx_bulletins_group_created ON bulletins(group_name, created_date DESC);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_summaries_join_items ON summaries(id) WHERE summary_text IS NOT NULL AND summary_text != '';
CREATE INDEX IF NOT EXISTS idx_items_summaries_join ON items(id, feed_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_feeds_items_join ON feeds(id, slug);