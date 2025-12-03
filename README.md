# Feed Summarizer

Funny story: This was mostly a vibe-coded project that got out of hand. It actually started as a Node-RED flow for personal use, then morphed into a Python script, and I thought it would both help me save time reading news in the mornings and make for a great demo of spec-driven development.

As a direct outcome of my swearing at various LLMs, it became this, which is, in a mouthful, an `asyncio`-based background service that fetches multiple RSS/Atom (and optional Mastodon) sources, stores raw items in SQLite, generates AI summaries (Azure OpenAI), groups them into bulletins, and publishes both HTML and RSS outputs (optionally uploading to Azure Blob Static Website hosting).

The pipeline is designed for efficiency (conditional fetching, batching, backoff) and the output is tailored to my reading habits (three "bulletins" per day that group items by topic, each bulletin published as both HTML and an RSS entry).

Most of the implementation started as a vibe-coded prototype, with some manual tweaking here and there, but it now has extensive error handling, logging, and observability hooks for Azure Application Insights via OpenTelemetry, and it publishes to Azure Blob Storage for publishing the results because there is no way I am letting this thing run a web server.

It is also deployable as a Docker Swarm service using `kata`, a private helper tool used for my own infrastructure.

## Contents

1. Quickstart (5 commands)
2. Features
3. Architecture (high‑level)
4. Configuration & Environment
5. Running (one‑shot vs scheduled vs step modes)
6. Publishing Outputs (HTML / RSS / passthrough / Azure)
7. Age Window & Retention
8. Telemetry (opt‑out / service naming)
9. Troubleshooting
10. Roadmap Snapshot

## 1. Quickstart (5 commands)

```bash
python -m venv .venv              # 1. Create virtualenv
source .venv/bin/activate         # 2. Activate it
pip install -r requirements.txt   # 3. Install dependencies
cp feeds.yaml.example feeds.yaml  # 4. Seed a starter config (edit it)
python main.py run                # 5. One full pipeline run (fetch→summarize→publish→upload*)
```

(*) Azure upload happens only if storage env vars are set; otherwise it is skipped automatically.

## 2. Features

- Concurrent conditional feed fetching (ETag / Last-Modified; respectful backoff & error tracking)
- Optional reader mode & GitHub README enrichment for richer summarization context
- AI summarization with per‑group introductions (opt‑in) via Azure OpenAI
- Topic/group bulletins rendered as responsive HTML + RSS 2.0 feeds
- SimHash-powered dedupe merges near-duplicate summaries and surfaces every source link
- Optional passthrough (raw) feeds with minimal processing
- Smart time‑based scheduling (timezone aware) plus interval overrides
- Azure Blob Storage upload with MD5 de‑dup (skip unchanged) & optional sync delete
- Graceful shutdown with executor timeouts and robust logging
- Config hot‑reload for feeds; caching of YAML & prompt data
- Observability hooks via OpenTelemetry (HTTP, DB, custom spans)

## 3. Architecture (High‑Level)

Module | Responsibility
-------|----------------
`fetcher.py` | Async retrieval, conditional GET headers, reader mode, error/backoff tracking.
`summarizer.py` | Batches new items, calls Azure OpenAI, retry & bisection for filtered content.
`publisher.py` | HTML bulletin + RSS feed generation, passthrough feeds, Azure upload, index pages.
`scheduler.py` | Time‑zone aware smart scheduling + status reporting.
`models.py` | Async database queue (SQLite WAL) + safe operation batching.
`config.py` | Centralized env/YAML/secrets loading + validation + normalization.
`telemetry.py` | OpenTelemetry initialization & instrumentation (`aiohttp`, sqlite, logging spans).
`main.py` | Orchestrator & CLI entry point; composes pipeline steps.

Processing flow (simplified):

```text
feeds.yaml -> fetcher -> items (SQLite) -> summarizer -> summaries -> publisher -> public/{bulletins,feeds}
                               ^                                           |
                               |_______ backoff + error counts ____________|
```

See `SPEC.md` for detailed sequence diagrams and data model rationale.

## 4. Configuration & Environment

Core files:

- `feeds.yaml` (sources, groups, schedule, passthrough) – see `feeds.yaml.example`.
- `prompt.yaml` (prompt templates for summarization & bulletins).
- `secrets.yaml` (or `.env`) for credentials; example in `secrets.yaml.example`.

Essential environment variables:

Variable | Purpose | Notes
---------|---------|------
AZURE_ENDPOINT | Azure OpenAI endpoint host (no scheme) | Auto-normalized (strip https://)
OPENAI_API_KEY | Azure OpenAI API key | Required for summaries
DEPLOYMENT_NAME | Model deployment (e.g. `gpt-4o-mini`) | Default: `gpt-4o-mini`
RSS_BASE_URL | Public base URL for generated links | Affects GUID/self links
DATABASE_PATH | SQLite path | Default: feeds.db
PUBLIC_DIR | Output directory root | Default: ./public
AZURE_STORAGE_ACCOUNT | Blob storage account | Optional
AZURE_STORAGE_KEY | Blob storage key | Optional
AZURE_UPLOAD_SYNC_DELETE | Delete remote orphans | Default: false (danger when true)
FETCH_INTERVAL_MINUTES | Base interval fallback | Default: 30
SCHEDULER_TIMEZONE | Override schedule TZ if not in feeds.yaml | Default: UTC
MAX_ITEMS_PER_FEED | Per-feed physical retention cap | Default: 400
SUMMARY_WINDOW_ITEMS | Unsummarized items per feed per summarizer pass | Default: 50
BULLETIN_SUMMARY_LIMIT | Summaries per HTML bulletin chunk | Default: 100
BULLETIN_PER_FEED_LIMIT | Max summaries a single feed can contribute to one chunk | Default: 40 (auto-reduced if many feeds)
BULLETIN_MAX_CHUNKS | Backlog chunks processed per run | Default: 5
`SIMHASH_HAMMING_THRESHOLD` | Max Hamming distance (0-64) for merging summaries | Default: 4 (set 0 to disable)
LOG_LEVEL | DEBUG / INFO / WARNING / ERROR | Default: INFO
DISABLE_TELEMETRY | Set true to disable all tracing/log export | Default: false

`secrets.yaml` may be either a top-level mapping or nested under `environment:`. Both are parsed and override `.env` and process env values.

## 5. Running

Mode | Command | What it does
-----|---------|-------------
One-shot full pipeline | `python main.py run` | Fetch → Summarize → Publish (HTML+RSS+passthrough) → Azure upload (if configured)
Scheduled (smart) | `python main.py scheduled` | Run continuously at times declared under `schedule:` in `feeds.yaml`
Show schedule | `python main.py schedule-status` | Print parsed schedule (with timezone)
Status snapshot | `python main.py status` | DB counts + output presence
Fetch only | `python main.py fetcher` | Just ingest feeds (no summarization/publish)
Summarize only | `python main.py summarizer` | Summarize new items (no publish)
Upload existing output | `python main.py upload` | Sync current `public/` tree to Azure only

Useful flags:

- `--no-publish` (with `run`) skip HTML/RSS generation.
- `--no-azure` disable Azure upload for that invocation.
- `--sync-delete` remove remote blobs not present locally (use cautiously).

Scheduling:

```yaml
schedule:
  timezone: Europe/Lisbon
  times:
    - "06:30"
    - "12:30"
    - "20:30"
```

If both `schedule.timezone` and `SCHEDULER_TIMEZONE` are set, the environment variable wins.

## 6. Publishing Outputs

Path | Description
-----|------------
`public/bulletins/*.html` | Per-group HTML bulletins (recent sessions, optional AI intro)
`public/feeds/*.xml` | Per-group summarized RSS feeds
`public/feeds/raw/*.xml` | Raw passthrough feeds (only for slugs listed under `passthrough:`)
`public/index.html` | Landing page / directory index

Retention & grouping:

- Bulletins group summaries by session/time window; large sessions split for readability.
- Passthrough feeds default limit is 50 items (configurable per slug).

Azure Upload:

- Provide `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY` (and optionally `AZURE_STORAGE_CONTAINER`, default `$web`).
- Set `AZURE_UPLOAD_SYNC_DELETE=true` to purge remote files not present locally.
- Upload step computes MD5 to skip unchanged blobs.

## 7. Age Window & Retention

Three complementary controls govern how long items stick around and which are summarized:

1. Time Window (feeds.yaml: `thresholds.time_window_hours`) – Unsummarized items older than this window are ignored when building prompts. Default: 48h. Raise temporarily for historical backfill.
2. Count-Based Physical Retention (env: `MAX_ITEMS_PER_FEED`) – After each fetch the newest N items per feed are kept (default 400). Older items beyond that per-feed cap are pruned. This prevents date-less feeds from re-surfacing old entries as “new” after day-based purges.
3. Summary Window (env: `SUMMARY_WINDOW_ITEMS`) – At most the newest N unsummarized items per feed are considered in a single summarizer pass (default 50). Larger backlogs are processed gradually across runs.

Optional long-term aging still applies via `ENTRY_EXPIRATION_DAYS` (default 365, fetcher maintenance) to trim truly old data if you run this for months.

feeds.yaml snippet (still supports thresholds for the time window & bulletin retention days):

```yaml
thresholds:
  time_window_hours: 48    # summarizer input recency filter
  retention_days: 7        # bulletin & legacy aging (raw item day purge now superseded by count-based pruning)
```

Environment overrides (set in `.env` or shell):

```bash
MAX_ITEMS_PER_FEED=400        # per-feed physical cap
SUMMARY_WINDOW_ITEMS=50       # per-feed prompt size cap
```

Operational guidance:

- If some feeds are extremely high volume, lower `MAX_ITEMS_PER_FEED` (e.g. 200) for faster turnover.
- To accelerate clearing a backlog, temporarily raise `SUMMARY_WINDOW_ITEMS` (e.g. to 80) then revert to keep prompts small.
- For historical bulk summarization, raise `time_window_hours` first; count cap ensures DB won't explode.
- High-fanout bulletin groups can now drain backlog fairly: adjust `BULLETIN_SUMMARY_LIMIT` (per chunk), `BULLETIN_PER_FEED_LIMIT` (per feed) and `BULLETIN_MAX_CHUNKS` (safety cap) so quieter feeds still get airtime even when paired with firehoses.

Edge cases:

- Feeds with no reliable pub dates fall back to ingestion timestamp; count-based retention ensures they stay recorded and won't churn.
- If Azure content filtering splits batches, the bisect logic still respects the summary window (post-filter items consume part of the window as usual).

Adjust these values and restart to apply. The fetcher handles pruning; the summarizer reads window sizes dynamically each pass.

## 8. Telemetry

Feature | How
--------|-----
Disable all telemetry | `DISABLE_TELEMETRY=true`
Service name override | `OTEL_SERVICE_NAME=feed-summarizer-prod`
Environment tag | `OTEL_ENVIRONMENT=production`
Azure exporter | Provide `APPLICATIONINSIGHTS_CONNECTION_STRING` (or legacy instrumentation key)

If no connection string, spans stay in-process (no console spam). Logs can also be exported when Azure exporter is available.

## 9. Troubleshooting

Symptom | Cause | Fix
--------|-------|----
Empty summaries | Missing / bad Azure config | Check endpoint host (no scheme), key, deployment
Few bulletin items | Items filtered / no new content | Verify fetcher logs & summary successes
Broken feed links | Wrong `RSS_BASE_URL` | Set correct public domain before publishing
Slow summarization | Rate limit / large content | Adjust `SUMMARIZER_REQUESTS_PER_MINUTE` / enable reader mode selectively
Missing Azure upload | Vars unset | Provide storage account + key or run without upload
Telemetry missing | Disabled or no exporter | Remove `DISABLE_TELEMETRY` / set connection string
Empty summaries with token usage | New structured response format returned parts list | Upgrade includes parser: ensure you pulled latest `ai_client.py`

## 10. Roadmap Snapshot

- Expand test coverage (`pytest`: fetcher scheduling, scheduler, Azure upload paths)
- Harden HTML sanitization (allowlist schemes/attributes)
- Optional container image & `pyproject.toml` packaging

---

## Contributions & License

See `LICENSE` (MIT) for licensing details. Contribution guidelines and a code of conduct will be documented in `CONTRIBUTING.md` and `CODE_OF_CONDUCT.md` as the project evolves. Security reports: (will be defined in `SECURITY.md`).

## Attribution

Some components and refactoring work were assisted by AI tooling; all code is reviewed for clarity and maintainability.
