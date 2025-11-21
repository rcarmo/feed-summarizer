# Feed Summarizer Specification

## Overview

The Feed Summarizer is an asynchronous RSS/Atom processing system that fetches feeds, generates AI summaries, and publishes both HTML bulletins and RSS feeds. It uses Azure OpenAI for summarization and can upload outputs to Azure Blob Storage. The system runs as a background service with a smart scheduler and a SQLite-backed persistence layer.

Key design tenets:

- Async-first using asyncio/aiohttp
- Minimal dependencies (see requirements.txt) and functional-style modules
- Robust error handling and observability
- Efficient I/O (HTTP conditional requests, atomic writes, selective uploads)

## Key features

- Orchestrated pipeline coordinated by `main.py`
- Async fetching, summarization, and publishing
- Configurable fetch intervals and per-feed schedules
- Decorator-based OpenTelemetry tracing with Azure Application Insights export
- Thread-safe SQLite operations via a database queue and WAL
- Comprehensive error handling and content sanitization
- Reader mode extraction for selected feeds; HTML-to-Markdown storage
- HTTP conditional requests and exponential backoff retries
- AI-powered summarization and topic grouping via Azure OpenAI
- Session-based bulletins with cached introductions
- Multi-format publishing (HTML + RSS)
- Azure Blob upload with hash-based deduplication (optional)
- Per-feed scoped publishing (only affected groups update)
- Passthrough raw feeds for selected source slugs
- Content filter resilience (detect and bisect 400 policy failures)

## Architecture

### Modules

- `fetcher.py`: Fetch feeds with conditional requests and store items
- `summarizer.py`: Generate summaries using Azure OpenAI; handles content filters and bisection
- `publisher.py`: Publish HTML bulletins and RSS feeds; introduction caching; per-feed scoping; Azure upload
- `models.py`: Database access helpers and queue; bulletin/session management
- `scheduler.py`: Global and per-feed schedule/interval handling
- `azure_storage.py`: Minimal Azure Blob Storage REST client used by the uploader
- `config.py`: Configuration loading, environment handling, endpoint normalization
- `utils.py`: Common helpers (sanitization, atomic writes, hashing)

### Workflow

1. Fetch: `fetcher.py` pulls new items from `feeds.yaml` sources using conditional HTTP and stores them in SQLite.
2. Summarize: `summarizer.py` builds prompts, calls Azure OpenAI via `llm_client.py`, and stores summaries/topics.
3. Publish HTML/RSS: `publisher.py` creates session bulletins per group with AI introductions and generates RSS feeds.
4. Passthrough: `publisher.py` optionally emits raw passthrough feeds for selected slugs.
5. Upload: `publisher.py` (via `AzureStorageUploader` and `azure_storage.py`) can push changed files to Azure Blob Storage.

### Fetching details

- Validates and sends If-Modified-Since/If-None-Match based on stored `etag` and `last_modified`.
- Handles 304 efficiently and uses error-count-based backoff on failures.
- Supports optional proxy configuration from `feeds.yaml` (including Tor via `tor` service).
- Reader mode extraction for select feeds; HTML sanitized and stored as Markdown.

### Summarization details

- Queues unsummarized items, trims inputs, constructs prompts.
- Calls Azure OpenAI asynchronously via `llm_client.chat_completion`.
- Detects content filter / policy refusals and returns `None` or raises `ContentFilterError` as appropriate.
- Recursively bisects batches on filter failures to salvage non-offending items; logs skipped IDs.
- Stores summaries with topics; marks items as summarized.

### Publishing details

- Session bulletins keyed to a time window; large sessions can be split for readability.
- AI introductions generated once per session and cached for reuse.
- Per-feed scoped publishing: in per-feed runs, only groups intersecting changed slugs are rebuilt.
- Diagnostics in logs: per-topic and per-feed counts; warns on unusually small bulletins.

## Scheduled operation

Define times in `feeds.yaml` (legacy list format):

```yaml
schedule:
  - time: "06:30"
  - time: "12:30"
  - time: "20:30"
```

Or using the newer mapping form with timezone:

```yaml
schedule:
  timezone: Europe/Lisbon
  times:
    - "06:30"
    - "12:30"
    - "20:30"
```

Per-feed schedules and intervals:

- Per-feed schedules: `feeds.<slug>.schedule: [{ time: "HH:MM" }, ...]`
- Per-feed intervals: `feeds.<slug>.interval_minutes` (alias: `refresh_interval_minutes`)
- Scheduler computes due feeds from `last_fetched` and runs per-feed pipelines; publishers respect per-feed scoping

Publishing cadence and timing:

- Global runs (top-level `schedule:` entries) execute the full pipeline: fetch → summarize → publish → upload.
- Per-feed runs (triggered by per-feed schedules or interval checks) also execute the full pipeline but scoped to selected slugs.
- If `SCHEDULER_RUN_IMMEDIATELY=true` (or `FORCE_REFRESH_FEEDS=true`), the scheduler performs a warm run on startup; in practice this runs the same pipeline, and publishing may occur immediately depending on configuration.

Run scheduler:

```bash
python main.py scheduled
python main.py schedule-status
python main.py status
```

## Mastodon list feeds

Configure a Mastodon list as a feed (no extra deps):

```yaml
feeds:
  masto_lobsters:
    type: mastodon
    url: https://mastodon.social/api/v1/timelines/list/46540
    title: Lobsters on Mastodon
    token_env: MASTODON_TOKEN   # or token: "..."
    limit: 40                    # optional; defaults to 40
    summarize: false             # default for mastodon feeds; set true to opt-in
```

Notes:

- Fetcher uses the token to call Mastodon and stores statuses as items
- HTML includes boosts, replies, CWs, attachments, and counters
- Mastodon feeds are excluded from summarization by default

## Hiding groups from index pages

Hide specific summary groups from the HTML and RSS index pages without disabling generation:

```yaml
summaries:
  technews:
    feeds: "teksapo, pplware"
    hidden: true          # hide from index pages (aliases: visible: false, hide_from_index: true)
```

Notes:

- Affects only `public/feeds/index.html` and `public/bulletins/index.html`
- Group pages like `public/bulletins/<group>.html` and `public/feeds/<group>.xml` are still generated

## Passthrough (raw) feeds

Publish raw, non-summarized RSS for selected slugs:

```yaml
passthrough:
  - masto_lobsters    # simple list
  raw_news:
    limit: 50
    title: "Raw News"
```

Outputs are written to `public/feeds/raw/<slug>.xml` and linked from the RSS index.

## Configuration

Environment variables (subset):

- DATABASE_PATH: path to SQLite file (default: `feeds.db`).
- FETCH_INTERVAL_MINUTES: base interval fallback for fetcher (default: `30`).
- ENTRY_EXPIRATION_DAYS: retention for items (default: `365`).
- AZURE_ENDPOINT: Azure OpenAI endpoint host (preferred) or full https URL; normalized internally.
- OPENAI_API_KEY: Azure OpenAI API key.
- DEPLOYMENT_NAME: model deployment (default: `gpt-4o-mini`).
- OPENAI_API_VERSION: API version (taken from env; see README for current default).
- AZURE_STORAGE_ACCOUNT / AZURE_STORAGE_KEY / AZURE_STORAGE_CONTAINER (optional): Azure Blob Storage configuration.
- RSS_BASE_URL: base URL for RSS links.
- SCHEDULER_TIMEZONE: scheduler timezone override (environment wins over `schedule.timezone` when both are set).
- SCHEDULER_RUN_IMMEDIATELY: if true, run the pipeline once immediately on boot before entering the schedule loop.
- FORCE_REFRESH_FEEDS: legacy flag; treated similarly to `SCHEDULER_RUN_IMMEDIATELY` for an immediate run.

`feeds.yaml` thresholds complement the environment toggles:

- `thresholds.time_window_hours`: ignore items older than this many hours when summarizing (default `48`).
- `thresholds.retention_days`: keep published bulletins/feeds for this many days (default `7`).
- `thresholds.initial_fetch_items`: on a brand-new feed, accept up to N most recent entries even if outside the time window (default `10`; set `0` to disable the bootstrap).

## Database

SQLite with WAL via `models.py` queue. Core tables:

- feeds: sources and HTTP state
- items: fetched entries (Markdown)
- summaries: AI summaries with topics and timestamps
- bulletins: group + session records with intro, counts, and feed_slug set
- bulletin_summaries: join table for bulletin membership

See `schema.sql` for the full DDL.

## References

- HTTP conditional request best practices: [http://rachelbythebay.com/w/2023/01/18/http/](http://rachelbythebay.com/w/2023/01/18/http/)
- Feed reader behavior best practices: [http://rachelbythebay.com/w/2024/05/27/feed/](http://rachelbythebay.com/w/2024/05/27/feed/)
- Feed reader scoring criteria: [http://rachelbythebay.com/w/2024/05/30/fs/](http://rachelbythebay.com/w/2024/05/30/fs/)
- Azure OpenAI API reference: [https://learn.microsoft.com/azure/ai-services/openai/reference](https://learn.microsoft.com/azure/ai-services/openai/reference)
- Azure Blob Storage REST API: [https://docs.microsoft.com/rest/api/storageservices/blob-service-rest-api](https://docs.microsoft.com/rest/api/storageservices/blob-service-rest-api)
