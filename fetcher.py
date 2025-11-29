#!/usr/bin/env python3
"""
RSS Feed fetcher and content processor.

This module fetches RSS feeds, extracts full article content using readability,
and processes items for downstream summarization. It handles rate limiting,
retry logic, and content extraction for reliable feed processing.
"""

from time import time, mktime
from asyncio import create_task, get_event_loop, sleep, wait_for, TimeoutError, CancelledError, Semaphore, gather
from aiohttp import ClientSession
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime, format_datetime
from pypdf import PdfReader
import io
from hashlib import md5
import os

from config import config, get_logger
from telemetry import get_tracer, init_telemetry, trace_span
from models import DatabaseQueue
from utils import RateLimiter, RetryHelper
from mastodon import fetch_list_timeline, render_status_html

import traceback
import re
import yaml
import feedparser
from bs4 import BeautifulSoup
from readability import Document
from utils import clean_html_to_markdown
from typing import Any, Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from functools import partial
from aiohttp import ClientError

# Module-specific logger
logger = get_logger("fetcher")
init_telemetry("feed-summarizer-fetcher")
_tracer = get_tracer("fetcher")

# Processing constants
HOUR_IN_SECONDS = 3600
MAX_BACKOFF_HOURS = 24
MAIN_LOOP_INTERVAL_HOURS = 2
SECONDS_PER_MINUTE = 60
DAILY_REPORT_THRESHOLD_SECONDS = 60  # Run daily report if within first minute of day

# HTTP status codes
HTTP_OK = 200
HTTP_NOT_MODIFIED = 304
HTTP_TOO_MANY_REQUESTS = 429


class FeedFetcher:
    def __init__(self) -> None:
        self.executor = ThreadPoolExecutor()
        self.db = None  # Initialize as None, will be set in initialize()
        self.reader_rate_limiter = RateLimiter(config.READER_MODE_REQUESTS_PER_MINUTE)
        self.retry_helper = RetryHelper(max_retries=config.MAX_RETRIES, base_delay=config.RETRY_DELAY_BASE)
        self._proxy_warning_slugs: Set[str] = set()
        self._proxy_usage_logged: Set[str] = set()
        
    async def initialize(self) -> None:
        """Initialize the database connection."""
        self.db = DatabaseQueue(config.DATABASE_PATH)
        await self.db.start()
        logger.info("FeedFetcher initialized")

    def calculate_backoff_delay(self, error_count: int) -> float:
        """Calculate exponential backoff delay based on error count."""
        if error_count == 0:
            return 0
        
        # Exponential backoff: 2^(error_count-1) hours, capped at 24 hours
        base_delay = HOUR_IN_SECONDS  # 1 hour in seconds
        delay = base_delay * (2 ** (error_count - 1))
        max_delay = MAX_BACKOFF_HOURS * HOUR_IN_SECONDS  # 24 hours
        
        return min(delay, max_delay)

    async def should_fetch_feed(self, feed_id: int) -> bool:
        """Determine if a feed should be fetched based on error history."""
        try:
            error_info = await self.db.execute('get_feed_error_info', feed_id=feed_id)
            error_count = error_info.get('error_count', 0)
            
            if error_count == 0:
                return True
            
            last_fetched = await self.db.execute('get_feed_last_fetched', feed_id=feed_id)
            if not last_fetched:
                return True
            
            backoff_delay = self.calculate_backoff_delay(error_count)
            time_since_last_fetch = int(time()) - last_fetched
            
            should_fetch = time_since_last_fetch >= backoff_delay
            
            if not should_fetch:
                remaining_time = backoff_delay - time_since_last_fetch
                logger.info(f"Feed ID {feed_id} in backoff (error count: {error_count}), "
                           f"next attempt in {remaining_time/HOUR_IN_SECONDS:.1f} hours")
            
            return should_fetch
            
        except (OSError, RuntimeError) as e:
            logger.error(f"Error checking if feed {feed_id} should be fetched: {e}")
            return True  # Default to fetching on error

    async def _handle_fetch_error(self, feed_id: int, error_message: str) -> None:
        """Handle feed fetch errors by updating error tracking and last_fetched timestamp."""
        try:
            # Get current error count
            error_info = await self.db.execute('get_feed_error_info', feed_id=feed_id)
            current_error_count = error_info.get('error_count', 0)
            new_error_count = current_error_count + 1
            
            # Update error tracking (this also updates last_fetched for backoff calculation)
            await self.db.execute('update_feed_error', 
                                feed_id=feed_id, 
                                error_count=new_error_count, 
                                last_error=error_message)
            
            # Calculate and log backoff information
            backoff_delay = self.calculate_backoff_delay(new_error_count)
            logger.warning(f"Feed ID {feed_id} error count increased to {new_error_count}. "
                          f"Next attempt in {backoff_delay/HOUR_IN_SECONDS:.1f} hours")
            
        except (OSError, RuntimeError) as e:
            logger.error(f"Error updating feed error tracking for feed ID {feed_id}: {e}")

    @trace_span(
        "fetch_feed",
        tracer_name="fetcher",
        attr_from_args=lambda self, slug, url, session: {
            "feed.slug": slug,
            "feed.url": url,
        },
    )
    async def fetch_feed(self, slug: str, url: str, session: ClientSession) -> None:
        """Fetch and process a single feed asynchronously."""
        logger.info(f"Considering feed: {slug} from {url}")
        
        # Setup feed and validate prerequisites
        feed_id = await self._setup_feed(slug, url)
        if not feed_id:
            return
        
        # Check if we should fetch this feed based on timing and error backoff
        if not await self._should_skip_feed_fetch(feed_id, slug):
            return
        
        # Get feed configuration
        feed_config = await self.get_feed_config(slug)
        post_process = feed_config.get('post_process', False)
        reader_mode = feed_config.get('reader_mode', False)
        feed_type = (feed_config.get('type') or '').lower()
        proxy_url = self._resolve_proxy_url(slug, feed_config)
    # Attributes are recorded via decorators; avoid manual span manipulation

        self._log_feed_processing_config(slug, post_process, reader_mode)
        
        # Branch on feed type
        if feed_type == 'mastodon':
            await self._fetch_mastodon_list(feed_id, slug, url, feed_config, session, proxy_url=proxy_url)
        else:
            logger.info(f"Fetching feed: {slug} from {url}")
            # Fetch the feed content (instrumented via decorator)
            content = await self._fetch_feed_content(feed_id, slug, url, session, proxy_url=proxy_url)
            if content is None:
                return
            # Parse and process the feed (instrumented via decorator)
            await self._parse_and_process_feed(feed_id, slug, content, post_process, reader_mode, session, proxy_url)
        
        # Update success status
        await self.db.execute('update_last_fetched', feed_id=feed_id)
        await self.db.execute('reset_feed_error', feed_id=feed_id)

    async def _setup_feed(self, slug: str, url: str) -> int | None:
        """Setup feed in database and return feed_id."""
        # Register the feed if it doesn't exist
        await self.db.execute('register_feed', slug=slug, url=url)
        
        # Get the feed ID
        feed_id = await self.db.execute('get_feed_id', slug=slug)
        if not feed_id:
            logger.error(f"Could not get feed ID for {slug}")
            return None
        
        # Check if we should fetch this feed based on error backoff
        if not await self.should_fetch_feed(feed_id):
            return None
        
        return feed_id

    async def _should_skip_feed_fetch(self, feed_id: int, slug: str) -> bool:
        """Check if feed should be skipped based on timing (supports per-feed interval override)."""
        # Load per-feed config to check for interval override
        try:
            feed_cfg = await self.get_feed_config(slug)
        except Exception:
            feed_cfg = {}

        # Determine interval in minutes: per-feed override > alias > global default
        interval_minutes = feed_cfg.get('interval_minutes')
        if interval_minutes is None:
            interval_minutes = feed_cfg.get('refresh_interval_minutes', config.FETCH_INTERVAL_MINUTES)
        try:
            interval_minutes = int(interval_minutes)
        except Exception:
            interval_minutes = config.FETCH_INTERVAL_MINUTES

        # Check last fetched time
        last_fetched_timestamp = await self.db.execute('get_feed_last_fetched', feed_id=feed_id)
        current_time = int(time())

        # If we have never fetched before, proceed
        if not last_fetched_timestamp:
            return True

        # Respect FORCE_REFRESH_FEEDS to bypass interval check
        if not config.FORCE_REFRESH_FEEDS:
            elapsed = current_time - int(last_fetched_timestamp)
            threshold = interval_minutes * SECONDS_PER_MINUTE
            if elapsed < threshold:
                try:
                    last_str = datetime.fromtimestamp(int(last_fetched_timestamp)).strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    last_str = str(last_fetched_timestamp)
                logger.info(
                    f"Skipping {slug}, fetched too recently (last: {last_str}, interval: {interval_minutes}m)."
                )
                return False

        return True

    def _log_feed_processing_config(self, slug: str, post_process: bool, reader_mode: bool) -> None:
        """Log feed processing configuration."""
        if post_process:
            logger.info(f"Post-processing enabled for feed: {slug}")
        if reader_mode:
            logger.info(f"Reader mode enabled for feed: {slug}")

    def _normalize_http_date(self, date_value: Optional[str]) -> Optional[str]:
        """Normalize HTTP date strings to RFC 7231 format (GMT)."""
        if not date_value:
            return None
        try:
            dt = parsedate_to_datetime(date_value)
            if not dt:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
            return format_datetime(dt, usegmt=True)
        except (TypeError, ValueError, OverflowError) as exc:
            logger.debug(f"Unable to normalize HTTP date '{date_value}': {exc}")
            return None

    async def _prepare_request_headers(self, feed_id: int, slug: str) -> dict:
        """Prepare HTTP headers for conditional requests."""
        # Get cached HTTP headers for conditional requests
        etag = await self.db.execute('get_feed_etag', feed_id=feed_id)
        last_modified = await self.db.execute('get_feed_last_modified', feed_id=feed_id)
        
        # Prepare headers for the request
        headers = {'User-Agent': config.USER_AGENT}
        
        # Add conditional request headers if we have cached values
        if etag:
            # Use ETag as-is for If-None-Match header (handles both strong and weak ETags)
            # ETags should be properly quoted by the server, but handle unquoted ones gracefully
            if not (etag.startswith('"') or etag.startswith('W/"')):
                etag = f'"{etag}"'  # Quote unquoted ETags
            headers['If-None-Match'] = etag
            logger.debug(f"Using If-None-Match: {etag} for {slug}")
            
        if last_modified:
            normalized_last_modified = self._normalize_http_date(last_modified)
            if normalized_last_modified:
                headers['If-Modified-Since'] = normalized_last_modified
                logger.debug(f"Using If-Modified-Since: {normalized_last_modified} for {slug}")
            else:
                logger.warning(
                    f"Invalid Last-Modified format for {slug}, not sending header (stored value: {last_modified})"
                )
        
        return headers

    @trace_span(
        "fetch_http_feed",
        tracer_name="fetcher",
        attr_from_args=lambda self, feed_id, slug, url, session: {
            "http.url": url,
            "feed.id": int(feed_id) if feed_id else 0,
            "feed.slug": slug,
        },
    )
    async def _fetch_feed_content(self, feed_id: int, slug: str, url: str, session: ClientSession, proxy_url: Optional[str] = None) -> bytes | None:
        """Fetch feed content with retry logic and error handling."""
        try:
            headers = await self._prepare_request_headers(feed_id, slug)
            
            # Fetch the feed with retries using RetryHelper
            timeout_seconds = self._compute_timeout(proxy_url)
            proxy_label = self._summarize_proxy(proxy_url) if proxy_url else None
            for attempt in range(config.MAX_RETRIES + 1):
                try:
                    request_kwargs = {
                        'headers': headers,
                        'timeout': timeout_seconds,
                        'max_redirects': config.MAX_REDIRECTS,
                    }
                    if proxy_url:
                        request_kwargs['proxy'] = proxy_url
                        if attempt == 0 and proxy_label:
                            logger.info("Fetching feed %s via proxy %s", slug, proxy_label)
                    async with session.get(
                        url,
                        **request_kwargs,
                    ) as response:
                        # Handle 304 Not Modified responses
                        if response.status == HTTP_NOT_MODIFIED:
                            logger.info(f"Feed {slug} not modified since last fetch")
                            # Update last_fetched timestamp and reset error count since this is a successful response
                            await self.db.execute('update_last_fetched', feed_id=feed_id)
                            await self.db.execute('reset_feed_error', feed_id=feed_id)
                            return None
                            
                        # Handle 429 Too Many Requests responses
                        if response.status == HTTP_TOO_MANY_REQUESTS:
                            await self._handle_rate_limit_response(feed_id, slug, response)
                            return None
                            
                        if response.status != HTTP_OK:
                            error_msg = f"HTTP {response.status}"
                            logger.error(f"Error fetching {slug}: {error_msg}")
                            await self._handle_fetch_error(feed_id, error_msg)
                            return None
                        
                        # Get new header values to store for future conditional requests
                        new_etag = response.headers.get('ETag')
                        new_last_modified = response.headers.get('Last-Modified')
                        
                        content = await response.read()
                        
                        # Store the new HTTP cache headers
                        await self._store_response_headers(feed_id, slug, new_etag, new_last_modified)
                        
                        return content
                        
                except TimeoutError as e:
                    # aiohttp surfaces timeouts as asyncio.TimeoutError; treat similarly to ClientError but with clearer logging
                    logger.warning(
                        "Timeout fetching %s (attempt %d/%d, timeout=%ss): %s",
                        slug,
                        attempt + 1,
                        config.MAX_RETRIES,
                        config.HTTP_TIMEOUT,
                        e,
                    )
                    if attempt < config.MAX_RETRIES:
                        await self.retry_helper.sleep_for_attempt(attempt)
                        continue
                    error_msg = "Timed out"
                    await self._handle_fetch_error(feed_id, error_msg)
                    return None
                except ClientError as e:
                    detail = self._format_client_error(e)
                    if attempt < config.MAX_RETRIES:
                        logger.warning(
                            "Retry %d/%d for %s due to error: %s",
                            attempt + 1,
                            config.MAX_RETRIES,
                            slug,
                            detail,
                        )
                        await self.retry_helper.sleep_for_attempt(attempt)
                    else:
                        error_msg = f"Failed to fetch after {config.MAX_RETRIES} retries ({detail})"
                        logger.error(f"{error_msg}: {slug}")
                        await self._handle_fetch_error(feed_id, error_msg)
                        return None
                
        except TimeoutError as e:
            logger.error(f"Timeout while fetching feed {slug}: {e}")
            await self._handle_fetch_error(feed_id, "Timeout")
            return None
        except ClientError as e:
            detail = self._format_client_error(e)
            logger.error(f"Error fetching feed {slug}: {detail}")
            await self._handle_fetch_error(feed_id, f"Network error: {detail}")
            return None
        except (OSError, RuntimeError, ValueError) as e:
            logger.error(f"Unexpected error processing feed {slug}: {e}")
            logger.error(traceback.format_exc())
            await self._handle_fetch_error(feed_id, f"Unexpected error: {str(e)}")
            return None

    async def _handle_rate_limit_response(self, feed_id: int, slug: str, response) -> None:
        """Handle 429 Too Many Requests response."""
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                cooldown = int(retry_after)
            except ValueError:
                cooldown = config.MIN_COOLDOWN_PERIOD
        else:
            cooldown = config.MIN_COOLDOWN_PERIOD
            
        logger.warning(f"Received 429 Too Many Requests for {slug}, respecting Retry-After: {cooldown} seconds")
        
        # Get existing headers to preserve them
        etag = await self.db.execute('get_feed_etag', feed_id=feed_id)
        last_modified = await self.db.execute('get_feed_last_modified', feed_id=feed_id)
        
        # Store the cooldown period in database and update last_fetched 
        await self.db.execute('update_feed_headers', 
                           feed_id=feed_id, 
                           etag=etag,  # Keep existing etag
                           last_modified=last_modified)  # Keep existing last_modified
        
        # Track 429 as an error, but don't be overly aggressive with backoff
        error_info = await self.db.execute('get_feed_error_info', feed_id=feed_id)
        current_error_count = error_info.get('error_count', 0)
        # Only increment error count if we've had multiple 429s recently
        if current_error_count > 0:
            await self._handle_fetch_error(feed_id, f"Rate limited (429), retry after {cooldown}s")
        else:
            # First 429, just log it without incrementing error count
            await self.db.execute('update_feed_error', 
                                 feed_id=feed_id, 
                                 error_count=1,  # Set to 1 but don't increment
                                 last_error=f"Rate limited (429), retry after {cooldown}s")

    async def _store_response_headers(self, feed_id: int, slug: str, new_etag: str | None, new_last_modified: str | None) -> None:
        """Store HTTP response headers for future conditional requests."""
        normalized_last_modified = self._normalize_http_date(new_last_modified) if new_last_modified else None
        if new_etag or normalized_last_modified:
            await self.db.execute('update_feed_headers', 
                                feed_id=feed_id,
                                etag=new_etag,
                                last_modified=normalized_last_modified)
            if new_etag:
                logger.debug(f"Stored new ETag: {new_etag} for {slug}")
            if normalized_last_modified:
                logger.debug(f"Stored new Last-Modified: {normalized_last_modified} for {slug}")

    @trace_span(
        "parse_and_process_feed",
        tracer_name="fetcher",
        attr_from_args=lambda self, feed_id, slug, content, post_process, reader_mode, session: {
            "feed.id": int(feed_id) if feed_id else 0,
            "feed.slug": slug,
        },
    )
    async def _parse_and_process_feed(
        self,
        feed_id: int,
        slug: str,
        content: bytes,
        post_process: bool,
        reader_mode: bool,
        session: Optional[ClientSession],
        proxy_url: Optional[str] = None,
    ) -> None:
        """Parse feed content and process entries."""
        # Set safer parsing options for feedparser
        feedparser_options = {
            'sanitize_html': True,          # Enable built-in sanitization
            'resolve_relative_uris': True    # Resolve relative URIs
        }
            
        # Parse the feed - feedparser is not async, run in executor
        feed = await self.run_in_executor(lambda c: feedparser.parse(c, **feedparser_options), content)
            
        # Check feed format and log details
        feed_type = feed.version if hasattr(feed, 'version') else "Unknown"
        logger.info(f"Feed {slug} parsed as {feed_type} format")
        
        if feed.bozo and hasattr(feed, 'bozo_exception'):
            logger.warning(f"Feed parsing warning for {slug}: {feed.bozo_exception}")
            # Check for specific XML parsing issues
            if "entities" in str(feed.bozo_exception).lower():
                logger.warning(f"Possible XML entity expansion attack attempt in {slug}")
        
        # Update feed title if available
        if 'feed' in feed and 'title' in feed.feed:
            await self.db.execute('update_feed_title', feed_id=feed_id, title=feed.feed.title)
        
        # Process and save entries
        if 'entries' in feed and feed.entries:
            await self._process_feed_entries(feed_id, slug, feed.entries, post_process, reader_mode, session, proxy_url)
        else:
            logger.warning(f"No entries found in feed {slug}")

    @trace_span(
        "process_feed_entries",
        tracer_name="fetcher",
        attr_from_args=lambda self, feed_id, slug, entries, post_process, reader_mode, session: {
            "feed.id": int(feed_id) if feed_id else 0,
            "feed.slug": slug,
            "feed.entries.count": len(entries) if entries else 0,
        },
    )
    async def _process_feed_entries(
        self,
        feed_id: int,
        slug: str,
        entries: list,
        post_process: bool,
        reader_mode: bool,
        session: Optional[ClientSession],
        proxy_url: Optional[str] = None,
    ) -> None:
        """Process and save feed entries with reader mode support."""
        # Log reader mode rate limiting information
        if reader_mode:
            rate_interval = 60.0 / config.READER_MODE_REQUESTS_PER_MINUTE if config.READER_MODE_REQUESTS_PER_MINUTE > 0 else 0
            logger.info(f"Reader mode rate limit: {config.READER_MODE_REQUESTS_PER_MINUTE} requests per minute (one request every {rate_interval:.2f} seconds)")
        
        # Create a semaphore for reader mode concurrency control (separate from rate limiting)
        reader_semaphore = Semaphore(config.READER_MODE_CONCURRENCY)

        # FIRST PASS: Extract basic entry data and collect GUIDs + URLs for cross-feed dedup
        raw_entries = []
        all_guids = []
        all_urls = []
        
        for entry in entries:
            # Skip entries without links or with malformed links
            if 'link' not in entry or not entry.link or not entry.link.startswith(('http://', 'https://')):
                logger.warning(f"Skipping entry with invalid link in {slug}")
                continue
                
            title = entry.get('title', 'No Title')
            url = entry.link
            guid = self.get_guid(entry)
            body = self.extract_content(entry)
            pub_date = self.parse_date_enhanced(entry)
            # Extra safeguard: ensure a valid positive integer timestamp
            try:
                if not isinstance(pub_date, int) or pub_date <= 0:
                    pub_date = int(time())
            except Exception:
                pub_date = int(time())
            title, url, guid = self._normalize_entry_identity(title, url, guid)
            
            raw_entries.append({
                'entry': entry,
                'title': title,
                'url': url,
                'guid': guid,
                'body': body,
                'pub_date': pub_date
            })
            all_guids.append(guid)
            all_urls.append(url)
        
        # Check which GUIDs already exist in database
        existing_guids = await self.db.execute('check_existing_guids', feed_id=feed_id, guids=all_guids)
        logger.info(f"Found {len(existing_guids)} existing items out of {len(all_guids)} total items for {slug}")

        # Check if this is an initial fetch (no existing items for this feed)
        is_initial_fetch = len(existing_guids) == 0
        if is_initial_fetch:
            logger.info(
                "Initial fetch detected for %s - will allow up to %d most recent items regardless of time window",
                slug,
                config.INITIAL_FETCH_ITEM_LIMIT,
            )

        # Cross-feed global URL dedup: identify URLs already stored under ANY feed
        existing_urls_global = await self.db.execute('check_existing_urls', urls=all_urls)
        if existing_urls_global:
            logger.info(
                f"Cross-feed dedup: {len(existing_urls_global)} URLs already stored globally will be treated as existing for {slug}"
            )
        
        # SECOND PASS: Process entries, applying reader mode only to new items
        total_new_items = await self._process_entries_with_reader_mode(
            feed_id,
            slug,
            raw_entries,
            existing_guids,
            existing_urls_global,
            post_process,
            reader_mode,
            reader_semaphore,
            session,
            proxy_url,
            is_initial_fetch,
            config.INITIAL_FETCH_ITEM_LIMIT,
        )
        
        logger.info(f"Added total of {total_new_items} new items from {slug}")
        # Count-based retention pruning (only if we added new items)
        if total_new_items > 0 and self.db:
            try:
                deleted = await self.db.execute('prune_items_per_feed', feed_id=feed_id, max_items=config.MAX_ITEMS_PER_FEED)
                if deleted:
                    logger.info(f"Pruned {deleted} old items for {slug} (kept newest {config.MAX_ITEMS_PER_FEED})")
            except Exception as e:
                logger.debug(f"Prune skipped for {slug}: {e}")

    async def _process_entries_with_reader_mode(
        self,
        feed_id: int,
        slug: str,
        raw_entries: list,
        existing_guids: set[str],
        existing_urls_global: set[str],
        post_process: bool,
        reader_mode: bool,
        reader_semaphore: Semaphore,
        session: Optional[ClientSession],
        proxy_url: Optional[str] = None,
        is_initial_fetch: bool = False,
        initial_bootstrap_limit: int = 10,
    ) -> int:
        """Process entries with reader mode and return total new items saved."""
        entries_data = []
        reader_mode_count = 0
        processed_new_items = 0
        skipped_existing_items = 0
        skipped_outside_window = 0
        bootstrap_allowed_items = 0
        existing_guid_matches = 0
        existing_url_matches = 0
        total_new_items = 0
        outside_window_examples: List[str] = []
        duplicate_examples: List[str] = []
        new_entry_examples: List[str] = []
        
        # Calculate time window cutoff (items older than this will be skipped)
        time_window_seconds = config.TIME_WINDOW_HOURS * HOUR_IN_SECONDS
        cutoff_timestamp = int(time()) - time_window_seconds
        logger.info(
            "%s: evaluating %d entries (cutoff=%s, window=%sh, initial_fetch=%s)",
            slug,
            len(raw_entries),
            self._format_timestamp(cutoff_timestamp),
            config.TIME_WINDOW_HOURS,
            is_initial_fetch,
        )
        if raw_entries:
            newest_entry = max(raw_entries, key=lambda entry: entry['pub_date'])
            oldest_entry = min(raw_entries, key=lambda entry: entry['pub_date'])
            span_hours = (newest_entry['pub_date'] - oldest_entry['pub_date']) / HOUR_IN_SECONDS
            logger.info(
                "%s: entry timespan %s → %s (≈%.1fh)",
                slug,
                self._format_timestamp(oldest_entry['pub_date']),
                self._format_timestamp(newest_entry['pub_date']),
                span_hours,
            )
        
        # For initial fetch, preselect the most recent items eligible for bootstrap regardless of time window
        bootstrap_guids: Set[str] = set()
        if is_initial_fetch and initial_bootstrap_limit > 0:
            sorted_entries = sorted(raw_entries, key=lambda entry: entry['pub_date'], reverse=True)
            bootstrap_guids = {entry['guid'] for entry in sorted_entries[:initial_bootstrap_limit]}
            logger.debug(
                "Bootstrap allowance prepared for %d entries on %s",
                len(bootstrap_guids),
                slug,
            )
            
        for i, raw_entry in enumerate(raw_entries):
            title = raw_entry['title']
            url = raw_entry['url']
            guid = raw_entry['guid']
            body = raw_entry['body']
            pub_date = raw_entry['pub_date']
            
            # Skip items outside the time window (unless it's an initial fetch and we haven't reached the limit)
            is_recent_enough = pub_date >= cutoff_timestamp
            allow_bootstrap = is_initial_fetch and guid in bootstrap_guids
            if not is_recent_enough and not allow_bootstrap:
                skipped_outside_window += 1
                if len(outside_window_examples) < 5:
                    outside_window_examples.append(
                        f"{title[:80]} @ {self._format_timestamp(pub_date)}"
                    )
                continue
            if allow_bootstrap and not is_recent_enough:
                bootstrap_allowed_items += 1
            
            # Only apply reader mode if this is a new item
            # New if neither GUID exists for this feed nor URL exists globally
            guid_known = guid in existing_guids
            url_known_global = url in existing_urls_global
            is_new_item = not (guid_known or url_known_global)
            if not is_new_item and url_known_global and not guid_known and len(duplicate_examples) < 5:
                duplicate_examples.append(
                    f"{title[:80]} (url dup) @ {self._format_timestamp(pub_date)}"
                )
            
            if reader_mode and is_new_item:
                try:
                    # Apply rate limiting before acquiring semaphore
                    await self.reader_rate_limiter.acquire()
                    
                    async with reader_semaphore:
                        logger.info(f"Applying reader mode for NEW entry: {title}")
                        full_content: Optional[str] = None
                        if not session:
                            logger.error(f"Reader mode requested for {slug} but no HTTP session is available; skipping full fetch for {url}")
                        else:
                            full_content = await self.fetch_original_content(url, session, proxy_url)
                        if full_content:
                            body = full_content
                            logger.info(f"Successfully extracted full content for: {title}")
                            reader_mode_count += 1
                except (ClientError, OSError, ValueError, RuntimeError) as e:
                    logger.error(f"Error applying reader mode for {url}: {e}")
            elif reader_mode and not is_new_item:
                logger.debug(f"Skipping reader mode for existing item: {title}")
            
            # Apply any additional post-processing if needed
            if post_process:
                logger.info(f"Applying post-processing for entry: {title}")
                # Note: Additional post-processing logic can be implemented here
                # based on the specific requirements in the feed configuration
            
            # Only save new items to the database to avoid unnecessary operations
            if is_new_item:
                processed_new_items += 1
                # Limit field sizes for database safety
                title = title[:255] if title else "No Title"  # Truncate long titles (safety)
                url = url[:2048] if url else ""              # Reasonable URL length limit (safety)
                guid = guid[:64] if guid else ""             # MD5 is 32 chars, but allow for longer IDs (safety)
                # Don't store empty bodies
                if not body or len(body.strip()) == 0:
                    body = "<p>No content available</p>"
                
                entries_data.append({
                    'title': title,
                    'url': url,
                    'guid': guid,
                    'body': body,
                    'date': pub_date
                })
                if len(new_entry_examples) < 5:
                    new_entry_examples.append(f"{title[:80]} @ {self._format_timestamp(pub_date)}")
            else:
                skipped_existing_items += 1
                if guid_known:
                    existing_guid_matches += 1
                if url_known_global:
                    existing_url_matches += 1
                if len(duplicate_examples) < 5 and not url_known_global:
                    duplicate_examples.append(
                        f"{title[:80]} (guid dup) @ {self._format_timestamp(pub_date)}"
                    )
            
            # Save in batches to prevent data loss in case of interruption
            if len(entries_data) >= config.SAVE_BATCH_SIZE or i == len(raw_entries) - 1:
                if entries_data:
                    new_items = await self.db.execute('save_items', feed_id=feed_id, entries_data=entries_data)
                    total_new_items += new_items
                    logger.info(f"Saved batch of {len(entries_data)} NEW entries from {slug}")
                    entries_data = []  # Reset for next batch
        if entries_data:
            new_items = await self.db.execute('save_items', feed_id=feed_id, entries_data=entries_data)
            total_new_items += new_items
            logger.info(f"Saved final batch of {len(entries_data)} NEW entries from {slug}")
            entries_data = []
        if reader_mode:
            logger.info(f"Applied reader mode to {reader_mode_count} new items from {slug}")
        logger.info(
            "%s summary: processed=%d new_candidates=%d saved=%d existing=%d (guid=%d url=%d) outside_window=%d bootstrap=%d",
            slug,
            len(raw_entries),
            processed_new_items,
            total_new_items,
            skipped_existing_items,
            existing_guid_matches,
            existing_url_matches,
            skipped_outside_window,
            bootstrap_allowed_items,
        )
        if outside_window_examples:
            logger.info("%s outside-window samples: %s", slug, "; ".join(outside_window_examples))
        if duplicate_examples:
            logger.info("%s duplicate samples: %s", slug, "; ".join(duplicate_examples))
        if new_entry_examples:
            logger.info("%s new entry samples: %s", slug, "; ".join(new_entry_examples))
        return total_new_items

    @trace_span(
        "fetch_all_feeds",
        tracer_name="fetcher",
        attr_from_args=lambda self, only_slugs=None: {
            "feed.only_slugs": ",".join(only_slugs) if only_slugs else "",
        },
    )
    async def fetch_all_feeds(self, only_slugs: Optional[List[str]] = None) -> None:
        """Fetch configured feeds concurrently with rate limiting.

        Args:
            only_slugs: If provided, only fetch these feed slugs.
        """
        logger.info("Starting feed fetching")
        
        # Create a client session that will be used for all requests
        async with ClientSession() as session:
            # Set concurrency limit - don't overwhelm servers
            semaphore = Semaphore(5)  # Max 5 concurrent requests
            
            # Define the fetch function with semaphore
            async def fetch_with_semaphore(slug, url):
                async with semaphore:
                    await self.fetch_feed(slug, url, session)
            
            # Create tasks for all feeds
            tasks = []
            for slug, url in config.FEED_SOURCES.items():
                if only_slugs is not None and slug not in only_slugs:
                    continue
                tasks.append(create_task(fetch_with_semaphore(slug, url)))
            
            # Wait for all tasks to complete
            await gather(*tasks, return_exceptions=True)

    async def discover_feed_url(self, site_url: str, session: ClientSession) -> Optional[str]:
        """
        Properly discover a feed URL from a website using <link> tags.
        This follows best practices mentioned by Rachel Bythebay.
        """
        logger.info(f"Attempting to discover feed URL from: {site_url}")
        try:
            async with session.get(site_url, headers={'User-Agent': config.USER_AGENT}, timeout=config.HTTP_TIMEOUT) as response:
                if response.status != 200:
                    logger.error(f"Error accessing {site_url}: HTTP {response.status}")
                    return None
                
                content = await response.text()
                soup = BeautifulSoup(content, 'html.parser')
                
                # Look for the standard <link rel="alternate"> tags
                feed_links = []
                for link in soup.find_all('link', rel='alternate'):
                    type_attr = link.get('type', '')
                    if type_attr in ('application/rss+xml', 'application/atom+xml', 'application/rdf+xml'):
                        href = link.get('href')
                        if href:
                            # Make sure we have an absolute URL
                            if not href.startswith(('http://', 'https://')):
                                # Handle relative URLs
                                if href.startswith('/'):
                                    # Absolute path
                                    parsed_url = urlparse(site_url)
                                    base = f"{parsed_url.scheme}://{parsed_url.netloc}"
                                    href = f"{base}{href}"
                                else:
                                    # Relative path
                                    if site_url.endswith('/'):
                                        href = f"{site_url}{href}"
                                    else:
                                        href = f"{site_url}/{href}"
                            
                            feed_links.append({
                                'url': href,
                                'title': link.get('title', 'Unknown feed'),
                                'type': type_attr
                            })
                
                if feed_links:
                    # Prefer Atom over RSS if both are available
                    atom_feeds = [f for f in feed_links if 'atom' in f['type']]
                    if atom_feeds:
                        logger.info(f"Discovered Atom feed: {atom_feeds[0]['url']}")
                        return atom_feeds[0]['url']
                    
                    logger.info(f"Discovered feed: {feed_links[0]['url']}")
                    return feed_links[0]['url']
                
                logger.warning(f"No feed links found in {site_url}")
                return None
                
        except (ClientError, OSError, ValueError) as e:
            logger.error(f"Error discovering feed from {site_url}: {e}")
            return None

    async def run_in_executor(self, func, *args) -> Any:
        """Run a blocking function in a thread pool executor."""
        loop = get_event_loop()
        return await loop.run_in_executor(self.executor, partial(func, *args))

    async def get_feed_config(self, slug: str) -> Dict[str, Any]:
        """Get feed configuration from the feeds.yaml file as a dict."""
        try:
            feeds_path = config.FEEDS_CONFIG_PATH
            
            with open(feeds_path, 'r') as f:
                feed_config_data = yaml.safe_load(f)
            
            if feed_config_data and 'feeds' in feed_config_data:
                if slug in feed_config_data['feeds']:
                    feed_config = feed_config_data['feeds'][slug]
                    # Ensure we have the basic configuration with defaults
                    result = {
                        'post_process': feed_config.get('post_process', False),
                        'reader_mode': feed_config.get('reader_mode', False)
                    }
                    # Pass through extra fields (type, token, title, etc.)
                    if isinstance(feed_config, dict):
                        for k, v in feed_config.items():
                            if k not in result:
                                result[k] = v
                    return result
            
            # Return default configuration if feed not found or file doesn't exist
            return {
                'post_process': False,  # Default value
                'reader_mode': False    # Default value
            }
            
        except (OSError, PermissionError, yaml.YAMLError, KeyError) as e:
            logger.warning(f"Error loading feed config for {slug}: {e}")
            # Return default configuration on error
            return {
                'post_process': False,  # Default value
                'reader_mode': False    # Default value
            }

    @trace_span(
        "fetch_mastodon_list",
        tracer_name="fetcher",
        attr_from_args=lambda self, feed_id, slug, list_url, feed_config: {
            "feed.id": int(feed_id) if feed_id else 0,
            "feed.slug": slug,
            "mastodon.list_url": list_url,
        },
    )
    async def _fetch_mastodon_list(self, feed_id: int, slug: str, list_url: str, feed_config: Dict[str, Any], session: Optional[ClientSession], proxy_url: Optional[str] = None) -> None:
        """Fetch a Mastodon list timeline and store as feed items."""
        import os
        token = feed_config.get('token') or (lambda name: os.environ.get(name) if name else None)(feed_config.get('token_env'))
        title = feed_config.get('title') or f"Mastodon: {slug}"
        limit = int(feed_config.get('limit', 40))
        if not token:
            logger.error(f"Mastodon feed '{slug}' missing token; skipping")
            return
        try:
            statuses = await fetch_list_timeline(list_url, token=token, limit=limit, session=session, proxy_url=proxy_url)
            if not statuses:
                logger.info(f"No statuses returned for Mastodon feed {slug}")
                return

            # Update feed title
            await self.db.execute('update_feed_title', feed_id=feed_id, title=title)

            # Prepare entries and avoid duplicates by GUID (use status.uri)
            rendered = [render_status_html(s) for s in statuses]
            guids = [r['guid'] for r in rendered if r.get('guid')]
            existing = await self.db.execute('check_existing_guids', feed_id=feed_id, guids=guids)

            entries_data: List[Dict[str, Any]] = []
            for r in rendered:
                if r['guid'] in existing:
                    continue
                # Ensure reasonable limits and defaults (align with RSS path)
                title_v = (r['title'] or 'No Title')[:255]
                url_v = (r['url'] or '')[:2048]
                guid_v = (r['guid'] or '')[:64]
                body_v = r['body'] or '<p>No content available</p>'
                date_v = int(r['date']) if isinstance(r['date'], int) else int(datetime.now().timestamp())
                entries_data.append({
                    'title': title_v,
                    'url': url_v,
                    'guid': guid_v,
                    'body': body_v,
                    'date': date_v,
                })

            if entries_data:
                new_items = await self.db.execute('save_items', feed_id=feed_id, entries_data=entries_data)
                logger.info(f"Saved {new_items} NEW Mastodon entries for {slug}")
            else:
                logger.info(f"No new Mastodon entries to save for {slug}")

        except Exception as e:
            logger.error(f"Error fetching Mastodon list for {slug}: {e}")

    @trace_span(
        "reader_mode_fetch",
        tracer_name="fetcher",
        attr_from_args=lambda self, url, session: {
            "entry.url": url,
        },
    )
    async def fetch_original_content(self, url: str, session: ClientSession, proxy_url: Optional[str] = None) -> str | None:
        """Fetch the original content from a URL using the readability library."""
        logger.info(f"Fetching original content from: {url}")
        try:
            timeout_seconds = self._compute_timeout(proxy_url)
            request_kwargs = {
                'headers': {'User-Agent': config.USER_AGENT},
                'timeout': timeout_seconds,
            }
            if proxy_url:
                request_kwargs['proxy'] = proxy_url
            async with session.get(url, **request_kwargs) as response:
                if response.status != 200:
                    logger.error(f"Error fetching original content from {url}: HTTP {response.status}")
                    return None
                    
                html_content = await response.text()
                
                # Use readability to extract article content
                # Run in executor since readability parsing is CPU-bound
                result = await self.run_in_executor(self._parse_with_readability, html_content, url)
                
                # Convert the HTML to Markdown using our clean_html function
                if result:
                    markdown_content = self.clean_html(result, base_url=url)
                    return markdown_content
                return None
        except (ClientError, OSError, ValueError, RuntimeError) as e:
            logger.error(f"Error fetching original content from {url}: {e}")
            return None

    def _parse_with_readability(self, html_content: str, url: str) -> str | None:
        """Parse HTML content with readability library (runs in executor)."""
        try:
            article = Document(html_content)
            # Return the HTML content from readability
            return article.summary()
        except (ImportError, ValueError, RuntimeError) as e:
            logger.error(f"Error in readability parsing for {url}: {e}")
            return None

    def parse_date(self, date_str: Optional[str]) -> int:
        """Parse a date string into a Unix timestamp."""
        if not date_str:
            return int(time())  # Default to current time if no date
            
        try:
            # Try to parse using feedparser's date handler
            time_struct = feedparser._parse_date(date_str)
            if time_struct:
                return int(mktime(time_struct))
        except (ValueError, TypeError, AttributeError, OSError) as e:
            logger.debug(f"Failed to parse date '{date_str}' with feedparser: {e}")

        # Fallback to current time if parsing fails
        return int(time())

    def parse_date_enhanced(self, entry) -> int:
        """Parse publication date with enhanced error handling for various date formats."""
        current_time = int(time())

        date_fields = [
            'published',
            'updated',
            'created',
            'modified',
            'date',
            'pubDate',
            'pubdate',
            'issued',
        ]

        # Try the listed fields plus their *_parsed variants in priority order
        for field in date_fields:
            value = self._get_entry_value(entry, field)
            timestamp = self._date_value_to_timestamp(value)
            if timestamp:
                return timestamp

            parsed_value = self._get_entry_value(entry, f"{field}_parsed")
            timestamp = self._date_value_to_timestamp(parsed_value)
            if timestamp:
                return timestamp

        # Catch feeds that expose *_parsed without the base field (feedparser quirk)
        struct_fields = [
            'published_parsed',
            'updated_parsed',
            'created_parsed',
            'modified_parsed',
            'date_parsed',
        ]
        for field in struct_fields:
            value = self._get_entry_value(entry, field)
            timestamp = self._date_value_to_timestamp(value)
            if timestamp:
                return timestamp

        # Try to extract date from the guid if it looks like it contains a timestamp
        entry_id = self._get_entry_value(entry, 'id')
        if entry_id:
            date_patterns = [
                r'(\d{4})-(\d{2})-(\d{2})',
                r'(\d{4})/(\d{2})/(\d{2})'
            ]

            for pattern in date_patterns:
                match = re.search(pattern, entry_id)
                if match:
                    try:
                        year, month, day = map(int, match.groups())
                        if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                            dt = datetime(year, month, day, tzinfo=timezone.utc)
                            return int(dt.timestamp())
                    except (ValueError, TypeError, OSError) as e:
                        logger.debug(f"Failed to parse date components for '{entry_id}': {e}")

        return current_time

    def _get_entry_value(self, entry, field: str) -> Any:
        """Safely fetch feedparser entry fields with attribute or dict access."""
        if not field or entry is None:
            return None
        try:
            value = getattr(entry, field)
        except AttributeError:
            value = None

        if value is not None:
            return value

        getter = getattr(entry, 'get', None)
        if callable(getter):
            try:
                return getter(field)
            except KeyError:
                return None
        return None

    def _date_value_to_timestamp(self, value: Any) -> Optional[int]:
        """Convert assorted date representations into a Unix timestamp."""
        if value in (None, ''):
            return None

        if isinstance(value, (int, float)):
            try:
                timestamp = int(value)
                if timestamp > 0:
                    return timestamp
            except (ValueError, OSError):
                return None
            return None

        if isinstance(value, datetime):
            dt = value
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())

        if isinstance(value, (list, tuple)):
            try:
                return int(mktime(tuple(value)))
            except (OverflowError, ValueError, OSError, TypeError):
                return None

        if isinstance(value, str):
            return self._parse_date_string(value)

        return None

    def _parse_date_string(self, date_str: str) -> Optional[int]:
        parsers = (
            self._parse_with_feedparser,
            self._parse_with_email_utils,
            self._parse_with_custom_formats,
        )
        for parser in parsers:
            timestamp = parser(date_str)
            if timestamp is not None:
                return timestamp
        return None

    def _parse_with_feedparser(self, date_str: str) -> Optional[int]:
        try:
            time_struct = feedparser._parse_date(date_str)
            if time_struct:
                return int(mktime(time_struct))
        except (ValueError, TypeError, AttributeError, OSError):
            return None
        return None

    def _parse_with_email_utils(self, date_str: str) -> Optional[int]:
        try:
            dt = parsedate_to_datetime(date_str)
            if dt:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp())
        except (TypeError, ValueError, OverflowError):
            return None
        return None

    def _parse_with_custom_formats(self, date_str: str) -> Optional[int]:
        custom_formats = [
            "%d %b %Y %H:%M:%S %z",
            "%d %b %Y %H:%M:%S %Z",
            "%d %b %Y %H:%M:%S",
        ]
        for fmt in custom_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp())
            except (ValueError, TypeError):
                continue
        return None

    def clean_html(self, html_content: str, base_url: Optional[str] = None) -> str:
        """Clean and convert HTML to Markdown using shared sanitizer."""
        return clean_html_to_markdown(html_content, base_url=base_url)

    def extract_content(self, entry) -> str:
        """Extract the content from a feed entry and convert to Markdown."""
        # Try different possible content fields
        content = ""
        
        # Check for content field
        if 'content' in entry and entry.content:
            for content_item in entry.content:
                if 'value' in content_item:
                    content = content_item.value
                    break
        
        # If no content found, try summary or description
        if not content and hasattr(entry, 'summary'):
            content = entry.summary
            
        # Fall back to description
        if not content and 'description' in entry:
            content = entry.description
            
        # If we have content, convert HTML to Markdown
        if content:
            base_url = self._get_entry_value(entry, 'link')
            markdown_content = self.clean_html(content, base_url=base_url)
            return markdown_content
            
        return "No content available"

    def get_guid(self, entry) -> str:
        """Extract or generate a GUID for an entry."""
        # Try to use the provided guid
        if 'id' in entry:
            return entry.id
            
        # Generate a consistent hash from the URL
        if 'link' in entry:
            return md5(entry.link.encode()).hexdigest()
            
        # Fall back to title + date if available
        if 'title' in entry and 'published' in entry:
            combined = f"{entry.title}{entry.published}"
            return md5(combined.encode()).hexdigest()
            
        # Last resort: random based on current time
        return md5(str(time()).encode()).hexdigest()

    async def close(self) -> None:
        """Close connections and clean up resources."""
        if self.db:
            await self.db.stop()
        if self.executor:
            logger.info("Shutting down thread pool executor...")
            try:
                # Attempt graceful shutdown with 30-second timeout using asyncio
                await wait_for(
                    get_event_loop().run_in_executor(
                        None, lambda: self.executor.shutdown(wait=True)
                    ), 
                    timeout=30.0
                )
                logger.info("Thread pool executor shut down successfully")
            except TimeoutError:
                logger.warning("Thread pool executor shutdown timed out after 30 seconds")
                # Force shutdown by not waiting for remaining threads
                self.executor.shutdown(wait=False)
                logger.warning("Forced thread pool executor shutdown (threads may still be running)")
            except Exception as e:
                logger.error(f"Error during thread pool executor shutdown: {e}")
                # Ensure executor is marked as shut down even if there's an error
                self.executor.shutdown(wait=False)
        logger.info("FeedFetcher closed")

    async def extract_pdf_text(self, url: str, session: ClientSession, proxy_url: Optional[str] = None) -> Optional[str]:
        """Download and extract text from a PDF file."""
        try:
            request_kwargs = {
                'timeout': self._compute_timeout(proxy_url),
            }
            if proxy_url:
                request_kwargs['proxy'] = proxy_url
            async with session.get(url, **request_kwargs) as response:
                if response.status == 200:
                    pdf_data = await response.read()
                    reader = PdfReader(io.BytesIO(pdf_data))
                    text = "\n".join(page.extract_text() for page in reader.pages if page.extract_text())
                    return text
                else:
                    logger.warning(f"Failed to fetch PDF {url}: HTTP {response.status}")
        except Exception as e:
            logger.error(f"Error extracting text from PDF {url}: {e}")
        return None

    def _resolve_proxy_url(self, slug: str, feed_config: Dict[str, Any]) -> Optional[str]:
        """Determine the proxy URL to use for a feed, if any."""
        proxy_setting = feed_config.get('proxy') if isinstance(feed_config, dict) else None
        if proxy_setting in (None, False):
            return None

        proxy_candidate: Optional[str] = None
        if isinstance(proxy_setting, str):
            proxy_candidate = proxy_setting.strip() or None
            if not proxy_candidate:
                if slug not in self._proxy_warning_slugs:
                    logger.warning(f"Feed {slug} enabled proxy but provided an empty URL; skipping proxy")
                    self._proxy_warning_slugs.add(slug)
        elif isinstance(proxy_setting, dict):
            dict_url = proxy_setting.get('url')
            if isinstance(dict_url, str):
                proxy_candidate = dict_url.strip() or None
                if not proxy_candidate and slug not in self._proxy_warning_slugs:
                    logger.warning(f"Feed {slug} proxy configuration missing a valid url; skipping proxy")
                    self._proxy_warning_slugs.add(slug)
            elif proxy_setting.get('enabled') is True:
                proxy_candidate = getattr(config, 'PROXY_URL', None)
            else:
                if slug not in self._proxy_warning_slugs:
                    logger.warning(f"Feed {slug} proxy configuration is missing a url field; skipping proxy")
                    self._proxy_warning_slugs.add(slug)
        elif isinstance(proxy_setting, bool):
            if proxy_setting:
                proxy_candidate = getattr(config, 'PROXY_URL', None)
                if not proxy_candidate and slug not in self._proxy_warning_slugs:
                    logger.warning(f"Feed {slug} requested proxy routing but no proxy.url is configured in feeds.yaml")
                    self._proxy_warning_slugs.add(slug)
        else:
            if slug not in self._proxy_warning_slugs:
                logger.warning(f"Feed {slug} uses unsupported proxy configuration type {type(proxy_setting).__name__}; skipping proxy")
                self._proxy_warning_slugs.add(slug)

        if proxy_candidate:
            if slug not in self._proxy_usage_logged:
                logger.info(f"Proxy enabled for feed {slug}")
                self._proxy_usage_logged.add(slug)
            return proxy_candidate

        return None

    def _summarize_proxy(self, proxy_url: Optional[str]) -> Optional[str]:
        """Provide a redacted proxy identifier for logging."""
        if not proxy_url:
            return None
        try:
            parsed = urlparse(proxy_url)
            if parsed.scheme and parsed.hostname:
                host = parsed.hostname
                if parsed.port:
                    host = f"{host}:{parsed.port}"
                return f"{parsed.scheme}://{host}"
        except ValueError:
            return proxy_url
        return proxy_url

    def _compute_timeout(self, proxy_url: Optional[str]) -> int:
        """Return the HTTP timeout, scaling up when routing through a proxy."""
        base_timeout = max(int(config.HTTP_TIMEOUT), 1)
        if proxy_url:
            return base_timeout * 6
        return base_timeout

    def _format_client_error(self, error: ClientError) -> str:
        """Describe aiohttp client errors with any available status/errno."""
        parts: List[str] = [error.__class__.__name__]
        status = getattr(error, 'status', None)
        if status is not None:
            parts.append(f"status={status}")
        os_error = getattr(error, 'os_error', None)
        if os_error is not None:
            errno = getattr(os_error, 'errno', None)
            strerror = getattr(os_error, 'strerror', None)
            if errno is not None:
                parts.append(f"errno={errno}")
            if strerror:
                parts.append(str(strerror))
        message = str(error)
        if message:
            parts.append(message)
        return " ".join(parts)

    def _format_timestamp(self, timestamp: Optional[int]) -> str:
        """Return a human-readable UTC timestamp for diagnostics."""
        if timestamp in (None, ""):
            return "n/a"
        try:
            return datetime.fromtimestamp(int(timestamp), tz=timezone.utc).isoformat()
        except (OSError, OverflowError, ValueError, TypeError):
            return str(timestamp)

    def _normalize_entry_identity(self, title: Optional[str], url: Optional[str], guid: Optional[str]) -> tuple[str, str, str]:
        """Apply the same normalization used for storage so dedup logic stays consistent."""
        norm_title = (title or "No Title").strip()
        if not norm_title:
            norm_title = "No Title"
        norm_title = norm_title[:255]

        norm_url = (url or "").strip()
        if norm_url:
            norm_url = norm_url[:2048]

        norm_guid = (guid or "").strip()
        if norm_guid:
            norm_guid = norm_guid[:64]

        return norm_title, norm_url, norm_guid


@trace_span("fetcher.main_loop", tracer_name="fetcher")
async def main_async() -> None:
    """Main async function to run the feed fetcher."""
    fetcher = FeedFetcher()
    try:
        await fetcher.initialize()
        
        # Run maintenance task for old entries
        try:
            logger.info("Running database maintenance - checking for expired entries")
            expired_count = await fetcher.db.execute('expire_old_entries', expiration_days=config.ENTRY_EXPIRATION_DAYS)
            if expired_count > 0:
                logger.info(f"Expired {expired_count} old entries from the database")
        except (OSError, RuntimeError) as e:
            logger.error(f"Error during database maintenance: {e}")
        
        while True:
            # Fetch all feeds concurrently
            await fetcher.fetch_all_feeds()
            
            # Run maintenance task for old entries once a day
            if int(time()) % (MAX_BACKOFF_HOURS * SECONDS_PER_MINUTE * SECONDS_PER_MINUTE) < DAILY_REPORT_THRESHOLD_SECONDS:  # Run if within the first minute of the day
                try:
                    logger.info("Running daily database maintenance - checking for expired entries")
                    expired_count = await fetcher.db.execute('expire_old_entries', expiration_days=config.ENTRY_EXPIRATION_DAYS)
                    if expired_count > 0:
                        logger.info(f"Expired {expired_count} old entries from the database")
                except (OSError, RuntimeError) as e:
                    logger.error(f"Error during database maintenance: {e}")
            
            # Wait for the next scheduled run (2 hours)
            main_loop_interval_seconds = MAIN_LOOP_INTERVAL_HOURS * HOUR_IN_SECONDS
            logger.info(f"Sleeping for {main_loop_interval_seconds} seconds until next run")
            await sleep(main_loop_interval_seconds)  # Sleep for 2 hours
            
    except CancelledError:
        logger.info("Feed fetcher task was cancelled")
    except (OSError, RuntimeError, ValueError) as e:
        logger.error(f"Unexpected error in main async loop: {e}")
    finally:
        await fetcher.close()


@trace_span("fetcher.single_run", tracer_name="fetcher")
async def main_async_single_run() -> None:
    """Main async function to run the feed fetcher once."""
    fetcher = FeedFetcher()
    try:
        await fetcher.initialize()
        
        # Run feed fetching once
        await fetcher.fetch_all_feeds()
        
        # Run maintenance task for old entries if needed
        try:
            logger.info("Running database maintenance - checking for expired entries")
            expired_count = await fetcher.db.execute('expire_old_entries', expiration_days=config.ENTRY_EXPIRATION_DAYS)
            if expired_count > 0:
                logger.info(f"Expired {expired_count} old entries from the database")
        except (OSError, RuntimeError) as e:
            logger.error(f"Error during database maintenance: {e}")
            
    except CancelledError:
        logger.info("Feed fetcher task was cancelled")
    except (OSError, RuntimeError, ValueError) as e:
        logger.error(f"Unexpected error in fetcher: {e}")
    finally:
        await fetcher.close()

