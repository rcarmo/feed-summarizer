#!/usr/bin/env python3
"""
AI-powered news summarizer for RSS feeds.

This module provides intelligent summarization of news items from RSS feeds
"""

from json import loads, JSONDecodeError
from time import time
import traceback
from asyncio import create_task, get_event_loop, sleep, wait_for, TimeoutError, CancelledError, Semaphore, gather
from aiohttp import ClientSession, ClientError, ClientTimeout, ClientResponseError
from datetime import datetime
from functools import partial
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple, Any
import sys
import re
import yaml

# Import configuration, models, and shared utilities
from config import config, get_logger
from llm_client import chat_completion as ai_chat_completion
from telemetry import init_telemetry, get_tracer, trace_span
from models import DatabaseQueue
from utils import RateLimiter, RetryHelper, compute_simhash, encode_int64

# Module-specific logger
logger = get_logger("summarizer")
init_telemetry("feed-summarizer-summarizer")
_tracer = get_tracer("summarizer")

# Import shared error to avoid circulars
from errors import ContentFilterError

def _mask_secret(value: Optional[str], show: int = 4) -> str:
    """Mask a secret value for safe logging (keep only first/last few chars)."""
    if not value:
        return "<missing>"
    v = str(value)
    if len(v) <= show * 2:
        return "*" * len(v)
    return f"{v[:show]}***{v[-show:]}"

# Validate required configuration
def validate_configuration():
    """Validate critical configuration values for the summarizer."""
    errors = []
    
    if not config.AZURE_ENDPOINT:
        errors.append("AZURE_ENDPOINT environment variable not set")
    elif not config.AZURE_ENDPOINT.strip():
        errors.append("AZURE_ENDPOINT environment variable is empty")
        
    if not config.OPENAI_API_KEY:
        errors.append("OPENAI_API_KEY environment variable not set")
    elif not config.OPENAI_API_KEY.strip():
        errors.append("OPENAI_API_KEY environment variable is empty")
    elif len(config.OPENAI_API_KEY) < 20:  # Basic sanity check
        errors.append("OPENAI_API_KEY appears to be invalid (too short)")
        
    if not config.DEPLOYMENT_NAME:
        errors.append("DEPLOYMENT_NAME environment variable not set")
    elif not config.DEPLOYMENT_NAME.strip():
        errors.append("DEPLOYMENT_NAME environment variable is empty")
        
    if not config.DATABASE_PATH:
        errors.append("DATABASE_PATH not configured")
        
    if errors:
        for error in errors:
            logger.error(error)
        logger.error("Please set required environment variables in your .env file or environment")
        sys.exit(1)
    else:
        # Log a concise, sanitized configuration summary for diagnostics
        try:
            endpoint_preview = config.AZURE_ENDPOINT or ""
            # Avoid duplicating scheme in summary; only show host-ish
            endpoint_preview = endpoint_preview.replace("https://", "").replace("http://", "")
            logger.info(
                "Azure OpenAI config: endpoint=%s, deployment=%s, api_version=%s, key=%s",
                endpoint_preview,
                config.DEPLOYMENT_NAME,
                config.OPENAI_API_VERSION,
                _mask_secret(config.OPENAI_API_KEY)
            )
        except Exception as e:
            logger.debug(f"Failed to log Azure config summary: {e}")

# Validate configuration on startup
validate_configuration()

# Load feed list from configuration
def get_feed_slugs() -> List[str]:
    """Get the list of feed slugs to process from feeds.yaml.

    Defaults:
    - RSS/Atom feeds: summarized by default
    - Mastodon feeds (type: mastodon): NOT summarized by default
      unless summarize: true is explicitly set per feed
    """
    slugs: List[str] = []
    try:
        with open(config.FEEDS_CONFIG_PATH, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
        feeds_cfg = cfg.get('feeds', {}) if isinstance(cfg, dict) else {}
        for slug, fc in feeds_cfg.items():
            if not isinstance(fc, dict):
                # Old/simple style: just a URL
                slugs.append(slug)
                continue
            ftype = (fc.get('type') or '').lower()
            summarize = fc.get('summarize')
            if summarize is True:
                slugs.append(slug)
            elif summarize is False:
                continue
            else:
                # Default behavior if not specified
                if ftype == 'mastodon':
                    # Skip by default
                    continue
                slugs.append(slug)
    except Exception as e:
        logger.warning(f"Failed to load summarizer feed list from YAML, using defaults: {e}")
        # Fallback: keep previous behavior
        return list(config.FEED_SOURCES.keys())
    return slugs

def load_prompts() -> Dict[str, str]:
    """Load prompts from prompt.yaml configuration file."""
    try:
        prompt_path = config.PROMPT_CONFIG_PATH
        with open(prompt_path, 'r', encoding='utf-8') as f:
            prompts = yaml.safe_load(f)
        return prompts or {}  # Handle case where yaml.safe_load returns None
    except FileNotFoundError:
        logger.error(f"Prompt configuration file not found at {config.PROMPT_CONFIG_PATH}")
        return {}
    except PermissionError:
        logger.error(f"No permission to read prompt configuration file at {config.PROMPT_CONFIG_PATH}")
        return {}
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in prompt configuration file: {e}")
        return {}
    except OSError as e:
        logger.error(f"OS error reading prompt configuration file: {e}")
        return {}

# Configuration constants for content processing
CONTENT_TRIM_WORDS = 300  # Maximum words to send to AI for processing
DEFAULT_RATE_LIMIT_WAIT = 60  # Default wait time for rate limiting (seconds)
CONCURRENT_FEEDS_LIMIT = 2  # Maximum concurrent feed processing
FEED_PROCESSING_DELAY = 5  # Delay between feed processing (seconds)

# Regular expressions for cleaning up Markdown
MD_HEADING_PATTERN = re.compile(r'^#+\s+', re.MULTILINE)
MD_EMPHASIS_PATTERN = re.compile(r'(\*\*|__|\*|_|~~|`)')
MD_LINK_PATTERN = re.compile(r'\[([^\]]+)\]\([^)]+\)')
MD_IMAGE_PATTERN = re.compile(r'!\[[^\]]*\]\([^)]+\)')
MD_LIST_PATTERN = re.compile(r'^\s*[-*+]\s+', re.MULTILINE)
MD_NUMBERED_LIST_PATTERN = re.compile(r'^\s*\d+\.\s+', re.MULTILINE)
MD_CODE_BLOCK_PATTERN = re.compile(r'```[^`]*```', re.DOTALL)
MD_BLOCKQUOTE_PATTERN = re.compile(r'^>\s+', re.MULTILINE)


class NewsProcessor:
    """Processes news items and generates AI summaries."""
    
    def __init__(self):
        self.retries: int = 0
        self.user_input: str = ""
        self.feed_title: str = ""
        self.db: Optional[DatabaseQueue] = None
        self.executor = ThreadPoolExecutor()
        self.prompts: Dict[str, str] = load_prompts()
        self.rate_limiter = RateLimiter(config.SUMMARIZER_REQUESTS_PER_MINUTE)
        self.retry_helper = RetryHelper(
            max_retries=config.SUMMARIZER_MAX_RETRIES, 
            base_delay=config.SUMMARIZER_RETRY_DELAY_BASE
        )
        # Limit concurrent GitHub README fetches
        self._github_semaphore = Semaphore(3)

    async def initialize(self):
        """Initialize the processor with database connection."""
        self.db = DatabaseQueue(config.DATABASE_PATH)
        await self.db.start()
        logger.info("NewsProcessor initialized")

    async def close(self):
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
        logger.info("NewsProcessor closed")

    async def run_in_executor(self, func, *args):
        """Run a blocking function in a thread pool executor."""
        loop = get_event_loop()
        return await loop.run_in_executor(self.executor, partial(func, *args))
    
    def _parse_github_repo(self, url: str) -> Optional[Tuple[str, str]]:
        """Parse a GitHub repository from a URL, returning (owner, repo) if applicable.
        Accepts URLs like:
        - https://github.com/owner/repo
        - https://github.com/owner/repo/...
        Ignores gist and non-repo hosts.
        """
        try:
            if not url:
                return None
            u = urlparse(url)
            host = (u.netloc or "").lower()
            if host not in ("github.com", "www.github.com"):
                return None
            parts = [p for p in (u.path or "").split("/") if p]
            if len(parts) < 2:
                return None
            owner, repo = parts[0], parts[1]
            if owner and repo and repo != "blog":
                # Trim .git if present
                if repo.endswith('.git'):
                    repo = repo[:-4]
                return owner, repo
        except Exception:
            return None
        return None

    async def _fetch_github_readme(self, owner: str, repo: str, session: ClientSession) -> Optional[str]:
        """Fetch repository README content from GitHub raw endpoints.
        Tries HEAD, then common default branches, and common README filenames.
        Returns markdown/text content or None.
        """
        branches = ["HEAD", "main", "master"]
        filenames = [
            "README.md", "Readme.md", "readme.md",
            "README.rst", "README.txt", "README"
        ]
        headers = {"User-Agent": config.USER_AGENT}
        timeout = config.HTTP_TIMEOUT
        base = "https://raw.githubusercontent.com"
        for br in branches:
            for fn in filenames:
                url = f"{base}/{owner}/{repo}/{br}/{fn}"
                try:
                    async with self._github_semaphore:
                        async with session.get(url, headers=headers, timeout=timeout) as resp:
                            if resp.status == 200:
                                text = await resp.text()
                                logger.info(f"Fetched README for {owner}/{repo} from {br}/{fn}")
                                return text or None
                            elif resp.status in (403, 429):
                                # Likely rate limited; don't hammer further
                                logger.warning(f"GitHub rate/forbidden for {owner}/{repo}: HTTP {resp.status}")
                                return None
                except Exception as e:
                    # Soft-fail and try next candidate
                    logger.debug(f"Error fetching {url}: {e}")
                    continue
        logger.debug(f"No README found for {owner}/{repo}")
        return None

    async def _enrich_items_with_github_readme(self, items: List[Dict[str, Any]], session: ClientSession) -> List[Dict[str, Any]]:
        """Replace body with README when item URL points to a GitHub repository.
        Returns the mutated items list.
        """
        if not items:
            return items

        async def maybe_replace(item: Dict[str, Any]):
            url = item.get('url') or ""
            parsed = self._parse_github_repo(url)
            if not parsed:
                return
            owner, repo = parsed
            readme = await self._fetch_github_readme(owner, repo, session)
            if readme:
                item['body'] = readme
                logger.info(f"Using README.md content for summarization: {owner}/{repo}")

        # Run limited concurrency
        tasks = [maybe_replace(it) for it in items]
        await gather(*tasks, return_exceptions=True)
        return items
        
    def markdown_to_plain_text(self, markdown_text: str) -> str:
        """Convert Markdown to plain text by removing formatting elements."""
        if not markdown_text:
            return ""
        
        # Process the Markdown text to extract plain text
        text = markdown_text
        
        # Remove images
        text = MD_IMAGE_PATTERN.sub('', text)
        
        # Extract text from links [text](url) -> text
        text = MD_LINK_PATTERN.sub(r'\1', text)
        
        # Remove headings markers
        text = MD_HEADING_PATTERN.sub('', text)
        
        # Remove emphasis markers
        text = MD_EMPHASIS_PATTERN.sub('', text)
        
        # Remove list markers
        text = MD_LIST_PATTERN.sub('', text)
        text = MD_NUMBERED_LIST_PATTERN.sub('', text)
        
        # Remove code blocks
        text = MD_CODE_BLOCK_PATTERN.sub('', text)
        
        # Remove blockquotes
        text = MD_BLOCKQUOTE_PATTERN.sub('', text)
        
        # Clean up extra whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = text.strip()
        
        return text
        
    @trace_span(
        "format_and_trim",
        tracer_name="summarizer",
        attr_from_args=lambda self, items: {"items.count": len(items) if items is not None else 0},
    )
    async def format_and_trim_content(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Convert Markdown to plain text and trim content for each item."""
        formatted_items = []
        
        for item in items:
            # Convert Markdown to plain text - this can be CPU-bound for large content
            plain_text = await self.run_in_executor(self.markdown_to_plain_text, item['body'])
            
            # Trim to first N words to provide context to the AI while staying within limits
            words = plain_text.split()
            trimmed_text = ' '.join(words[:CONTENT_TRIM_WORDS]) if len(words) > CONTENT_TRIM_WORDS else plain_text
            
            formatted_items.append({
                'title': item['title'],
                'body': trimmed_text,
                'id': item['id']
            })
            
        return formatted_items

    def make_groups_of_key_value_pairs(self, items: List[Dict[str, Any]]) -> str:
        """Format items as key-value pairs for the LLM prompt."""
        result = ""
        
        for item in items:
            result += f"Title: {item['title']}\n"
            result += f"ID: {item['id']}\n"
            result += f"Body: {item['body']}\n\n"
            
        return result

    # Removed legacy _build_api_request_data and _make_api_request after migration to ai_client.chat_completion

    @trace_span(
        "azure_openai.completions",
        tracer_name="summarizer",
        attr_from_args=lambda self, prompt_text, session: {
            "azure.openai.deployment": config.DEPLOYMENT_NAME,
            "azure.openai.api_version": config.OPENAI_API_VERSION,
            "prompt.length": len(prompt_text or ""),
        },
    )
    async def call_azure_openai(self, prompt_text: str, session: ClientSession) -> Optional[str]:
        """Call Azure OpenAI API with prepared prompt using shared helper (falls back to detailed path for content filter)."""
        system_prompt = self.prompts.get('summaries', '')
        if not system_prompt:
            logger.error("No 'summaries' prompt found in configuration")
            return None
        self.user_input = prompt_text

        # Use shared helper (handles retries/backoff + content filter detection). Add system + user messages.
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt_text},
        ]
        await self.rate_limiter.acquire()
        try:
            return await ai_chat_completion(
                messages,
                purpose="summaries",
            )
        except ContentFilterError:
            # Propagate so bisect logic can split payload
            raise

    @trace_span(
        "summarize_subset",
        tracer_name="summarizer",
        attr_from_args=lambda self, items_subset, session, url_ids: {"items.count": len(items_subset) if items_subset is not None else 0},
    )
    async def _summarize_items_subset(self, items_subset: List[Dict[str, Any]], session: ClientSession, url_ids: Dict[int, str]) -> Tuple[str, List[Any], Dict[Any, Tuple[str, str, Optional[int]]]]:
        """Summarize a subset of items. May raise ContentFilterError. Returns markdown, seen_ids, summaries_dict."""
        prompt_text = self.make_groups_of_key_value_pairs(items_subset)
        json_content = await self.call_azure_openai(prompt_text, session)
        if not json_content:
            return "", [], {}
        try:
            markdown, seen_ids, summaries_dict = await self.group_by_topic_and_generate_markdown(json_content, url_ids)
            return markdown, seen_ids, summaries_dict
        except JSONDecodeError as e:
            logger.warning(f"JSON parsing error on subset of size {len(items_subset)}: {e}")
            return "", [], {}

    @trace_span(
        "bisect_summarize",
        tracer_name="summarizer",
        attr_from_args=lambda self, items_subset, session, url_ids: {"items.count": len(items_subset) if items_subset is not None else 0},
    )
    async def _bisect_summarize(self, items_subset: List[Dict[str, Any]], session: ClientSession, url_ids: Dict[int, str]) -> Tuple[str, List[Any], Dict[Any, Tuple[str, str, Optional[int]]], List[Any]]:
        """Recursively bisect items to avoid content filter. Returns markdown, summarized_ids, summaries_dict, filtered_ids."""
        n = len(items_subset)
        if n == 0:
            return "", [], {}, []
        # Try full subset first
        try:
            md, ids, sums = await self._summarize_items_subset(items_subset, session, url_ids)
            if ids:
                return md, ids, sums, []
            # No ids but no filter: nothing to do
            return "", [], {}, []
        except ContentFilterError as cf:
            if n == 1:
                # Single offending item; skip it
                off_id = items_subset[0]['id']
                logger.warning(f"Skipping item due to content filter: id={off_id}, title='{items_subset[0]['title']}'")
                return "", [], {}, [off_id]
            # Split and recurse
            mid = n // 2
            left = items_subset[:mid]
            right = items_subset[mid:]
            # Attributes are covered by decorator and logs
            md_l, ids_l, sums_l, filt_l = await self._bisect_summarize(left, session, url_ids)
            md_r, ids_r, sums_r, filt_r = await self._bisect_summarize(right, session, url_ids)
            # Merge results
            merged_md = "".join([md_l, md_r])
            merged_ids = ids_l + ids_r
            merged_sums = {**sums_l, **sums_r}
            merged_filt = filt_l + filt_r
            return merged_md, merged_ids, merged_sums, merged_filt

    async def group_by_topic_and_generate_markdown(self, json_content: str, url_ids: Dict[int, str]) -> Tuple[str, List[Any], Dict[Any, Tuple[str, str, Optional[int]]]]:
        """Parse JSON and generate markdown grouped by topic."""
        try:
            # Log the raw response for debugging
            logger.debug(f"Raw API response: {json_content}")
            
            # Clean up common JSON formatting issues
            cleaned_json = json_content
            if not cleaned_json.strip().startswith('['):
                # Try to extract just the JSON array part if it's embedded in other text
                json_match = re.search(r'\[(.*)\]', cleaned_json, re.DOTALL)
                if json_match:
                    cleaned_json = f"[{json_match.group(1)}]"
            
            # Remove any trailing commas before closing brackets (common JSON error)
            cleaned_json = re.sub(r',\s*}', '}', cleaned_json)
            cleaned_json = re.sub(r',\s*]', ']', cleaned_json)
            
            try:
                items = loads(cleaned_json)
            except JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON response (retryable error): {e}")
                logger.debug(f"Problematic JSON was: {cleaned_json}")
                # Re-raise as a retryable error instead of returning empty results
                raise e
            
            seen_ids = []
            topics = {}
            summaries_dict = {}  # Dictionary to store summaries for database
            
            # Create a set of valid IDs for faster lookup, ensure all IDs are strings
            valid_ids = {str(id_val) for id_val in url_ids.keys()}
            
            # Group items by topic
            for item in items:
                topic = item.get('topic')
                if not topic:
                    logger.warning(f"Item missing topic: {item}")
                    continue
                
                # Normalize ID to string for consistent comparison
                item_id = item.get('id')
                if item_id is None:
                    logger.warning(f"Item missing ID: {item}")
                    continue
                
                item_id_str = str(item_id)
                
                # Validate item
                if item_id_str not in valid_ids:
                    logger.warning(f"Invalid ID: {item_id} (not in database)")
                    continue
                    
                if "One or two sentence summary for Item" in item.get('summary', ''):
                    logger.warning(f"Incorrect summary format for ID {item_id}")
                    continue
                
                # Convert ID back to original type for lookup
                original_id = next((k for k in url_ids.keys() if str(k) == item_id_str), None)
                if original_id is None:
                    # This shouldn't happen given the check above, but just in case
                    continue
                    
                seen_ids.append(original_id)
                if topic not in topics:
                    topics[topic] = []
                
                # Save with the ID in its original type
                item['id'] = original_id
                topics[topic].append(item)
                
                # Store the summary text, topic, and simhash fingerprint for database insertion
                summary_text = item.get('summary', '')
                simhash_value = compute_simhash(summary_text) if summary_text else None
                summaries_dict[original_id] = (
                    summary_text,
                    topic,
                    encode_int64(simhash_value) if simhash_value is not None else None,
                )
            
            # Generate markdown
            markdown = ""
            for topic, items in topics.items():
                markdown += f"\n## {topic}\n\n"
                for item in items:
                    url = url_ids.get(item['id'])
                    markdown += f"- {item['summary']} ([link]({url}))\n"
            
            self.retries = 0  # Reset retries counter
            logger.info(f"Generated summary with {len(seen_ids)} items across {len(topics)} topics")
            return markdown, seen_ids, summaries_dict
            
        except JSONDecodeError:
            # Re-raise JSON errors so they can be retried at a higher level
            raise
        except Exception as e:
            logger.error(f"Unexpected error in processing summary: {e}")
            logger.error(traceback.format_exc())
            return "", [], {}

    def generate_summary_feed(self, markdown: str, feed_title: str) -> Dict[str, Any]:
        """Generate a summary feed entry."""
        summary_data = {
            'feed': "https://rss.carmo.io/feeds?slug=summaries",
            'url': f"https://rss.carmo.io/feeds?slug=summaries&_msgid={time()}",
            'title': feed_title,
            'body': markdown,
            'date': int(time())
        }
        
        logger.info(f"Title: {summary_data['title']}")


    @trace_span(
        "process_feed",
        tracer_name="summarizer",
        attr_from_args=lambda self, slug, session: {"feed.slug": slug},
    )
    async def process_feed(self, slug: str, session: ClientSession) -> int:
        """Process a single feed."""
        logger.info(f"Processing feed: {slug}")
    # Span is managed by decorator; avoid manual instrumentation inside function
        
        # Get or register feed ID for error tracking
        feed_id = await self.db.execute('get_feed_id', slug=slug)
        if not feed_id:
            # Register feed if it doesn't exist (needed for error tracking)
            feed_url = config.FEED_SOURCES.get(slug, f"https://unknown/{slug}")
            await self.db.execute('register_feed', slug=slug, url=feed_url)
            feed_id = await self.db.execute('get_feed_id', slug=slug)
        
        try:
            # Query the database for unsummarized items
            # Apply time window filtering using configured TIME_WINDOW_HOURS
            cutoff_hours = getattr(config, 'TIME_WINDOW_HOURS', 48)
            # Set dynamic per-call LIMIT using SUMMARY_WINDOW_ITEMS (passed via models override attr)
            if self.db:
                # Inject override attribute on db instance for this call scope
                try:
                    self.db.SUMMARY_WINDOW_ITEMS_OVERRIDE = config.SUMMARY_WINDOW_ITEMS
                except Exception:
                    pass
            items = await self.db.execute('query_raw_feeds', slugs=[slug], cutoff_age_hours=cutoff_hours)
            if not items:
                logger.info(f"No unsummarized items found within last {cutoff_hours}h for {slug}")
                return 0
            if not items:
                logger.info(f"No unsummarized items found for {slug}")
                return 0
                
            # Store feed IDs and URLs for later reference - use local variable to avoid race conditions
            url_ids = {item['id']: item['url'] for item in items}
            feed_title = items[0]['feed_title'] if items else "Summary"
            
            # Prefer README.md for GitHub URLs before formatting
            try:
                items = await self._enrich_items_with_github_readme(items, session)
            except Exception as e:
                logger.debug(f"GitHub README enrichment failed (continuing with original content): {e}")

            # Format and prepare content
            formatted_items = await self.format_and_trim_content(items)
            prompt_text = self.make_groups_of_key_value_pairs(formatted_items)
            
            # Implement retry logic for API call and JSON parsing
            retries = 0
            while retries <= config.SUMMARIZER_MAX_RETRIES:
                try:
                    # Call the API
                    json_content = await self.call_azure_openai(prompt_text, session)
                    
                    if json_content:
                        # This can raise JSONDecodeError which we want to retry
                        # Avoid manual span attribute setting; keep logs concise
                        markdown, seen_ids, summaries_dict = await self.group_by_topic_and_generate_markdown(json_content, url_ids)
                        if markdown and seen_ids:
                            self.generate_summary_feed(markdown, feed_title)
                            # Pass the summaries_dict to store summary text and topics
                            marked_count = await self.db.execute('verify_and_mark_as_summarized', ids=seen_ids, summaries=summaries_dict)
                            # Avoid manual span attribute setting; rely on return value and logs
                            
                            # Reset error count on successful processing
                            if feed_id:
                                await self.db.execute('reset_feed_error', feed_id=feed_id)
                            
                            return marked_count
                    
                    # If we get here, API call succeeded but returned no content or failed to parse
                    break
                    
                except ContentFilterError as cf:
                    # Use bisect strategy to salvage safe items and add placeholders for filtered ones
                    logger.warning(f"Content filter encountered for feed {slug}. Attempting to bisect items to find safe subset.")
                    md_b, ids_b, sums_b, filtered_ids = await self._bisect_summarize(formatted_items, session, url_ids)

                    # Build Safety section for filtered items (placeholders)
                    safety_md = ""
                    if filtered_ids:
                        safety_lines = []
                        # Map id->item for quick lookup
                        by_id = {it['id']: it for it in items}
                        for fid in filtered_ids:
                            it = by_id.get(fid)
                            if not it:
                                continue
                            url = url_ids.get(fid, "")
                            title_i = it.get('title') or 'No Title'
                            # Prefer per-item feed_title if available; else domain
                            source_i = it.get('feed_title') or (urlparse(url).netloc if url else 'Unknown source')
                            safety_lines.append(f"- Not summarized due to content policy — {source_i}: {title_i} ([link]({url}))")
                        if safety_lines:
                            safety_md = "\n## Safety\n\n" + "\n".join(safety_lines) + "\n"

                    combined_md = (md_b or "") + (safety_md or "")

                    # If we have any content (safe summaries or placeholders), emit the feed entry
                    if combined_md:
                        self.generate_summary_feed(combined_md, feed_title)

                    # Persist only the truly summarized items
                    marked_count = 0
                    if ids_b:
                        marked_count = await self.db.execute('verify_and_mark_as_summarized', ids=ids_b, summaries=sums_b)
                        if feed_id:
                            await self.db.execute('reset_feed_error', feed_id=feed_id)

                    if filtered_ids:
                        logger.warning(f"Included {len(filtered_ids)} item(s) as placeholders due to content filter: {filtered_ids}")

                    # Return count of actually summarized items (placeholders are not marked as summarized)
                    return marked_count
                except JSONDecodeError as e:
                    retries += 1
                    if retries > config.SUMMARIZER_MAX_RETRIES:
                        logger.error(f"JSON parsing failed after {config.SUMMARIZER_MAX_RETRIES} retries for feed {slug}: {e}")
                        break
                    
                    logger.warning(f"JSON parsing error for feed {slug}, retry {retries}/{config.SUMMARIZER_MAX_RETRIES}: {e}")
                    await self.retry_helper.sleep_for_attempt(retries - 1)
                    continue
            
            # If we get here, processing failed after retries
            if feed_id:
                await self._handle_processing_error(feed_id, slug, "Failed to generate valid summaries after retries")
            
            return 0
            
        except Exception as e:
            # Handle any processing errors with proper error tracking
            error_message = f"Feed processing error: {str(e)}"
            logger.error(f"Error processing feed {slug}: {e}")
            
            if feed_id:
                await self._handle_processing_error(feed_id, slug, error_message)
            
            return 0

    async def _handle_processing_error(self, feed_id: int, slug: str, error_message: str):
        """Handle feed processing errors with proper tracking."""
        try:
            # Get current error count
            error_info = await self.db.execute('get_feed_error_info', feed_id=feed_id)
            current_error_count = error_info.get('error_count', 0)
            new_error_count = current_error_count + 1
            
            # Update error tracking
            await self.db.execute('update_feed_error', 
                                feed_id=feed_id, 
                                error_count=new_error_count, 
                                last_error=error_message)
            
            logger.warning(f"Feed {slug} (ID: {feed_id}) error count increased to {new_error_count}: {error_message}")
            
        except Exception as e:
            logger.error(f"Failed to update error tracking for feed {slug}: {e}")

    @trace_span(
        "process_all_feeds",
        tracer_name="summarizer",
        attr_from_args=lambda self, only_slugs=None: {"feed.only_slugs": ",".join(only_slugs) if only_slugs else ""},
    )
    async def process_all_feeds(self, only_slugs: Optional[List[str]] = None):
        """Process all feeds with rate limiting using semaphore.

        Args:
            only_slugs: If provided, only process these feed slugs.
        """
        logger.info("Starting scheduled feed processing")
        
        # Use aiohttp session for all API calls
        async with ClientSession() as session:
            # Use semaphore to limit concurrent requests to the API
            semaphore = Semaphore(CONCURRENT_FEEDS_LIMIT)  # Process max N feeds concurrently
            
            # Define process function with semaphore
            async def process_with_semaphore(slug):
                async with semaphore:
                    await self.process_feed(slug, session)
                    # Add a delay between feeds to avoid overwhelming the API
                    await sleep(FEED_PROCESSING_DELAY)
            
            # Create tasks for all feeds
            tasks = []
            feed_slugs = get_feed_slugs()
            if only_slugs is not None:
                feed_slugs = [s for s in feed_slugs if s in only_slugs]
            for slug in feed_slugs:
                logger.info(f"Queueing feed: {slug}")
                tasks.append(create_task(process_with_semaphore(slug)))
            
            # Wait for all feeds to be processed
            await gather(*tasks, return_exceptions=True)

            # Count-based pruning now handled in fetcher; avoid aggressive day-based purge here.
            
        logger.info("Completed processing all feeds")

@trace_span("summarizer.main_loop", tracer_name="summarizer")
async def main_async():
    """Main async function to run the feed summarizer with scheduling loop."""
    processor = NewsProcessor()
    try:
        await processor.initialize()
        
        # Run immediately on startup
        await processor.process_all_feeds()
        
        # Schedule future runs using asyncio instead of the schedule library
        while True:
            # Get current time
            now = datetime.now()
            current_time = now.strftime("%H:%M")
            
            # Check if it's time to run
            scheduled_times = ["07:00", "12:00", "21:35"]
            
            if current_time in scheduled_times:
                logger.info(f"Running scheduled feed processing at {current_time}")
                await processor.process_all_feeds()
                # Wait a minute to avoid running multiple times during the same minute
                await sleep(60)
            else:
                # Check every minute
                await sleep(60)
                
    except CancelledError:
        logger.info("Summarizer task was cancelled")
    except Exception as e:
        logger.error(f"Unexpected error in main async loop: {e}")
        logger.error(traceback.format_exc())
    finally:
        await processor.close()


@trace_span("summarizer.single_run", tracer_name="summarizer")
async def main_async_single_run():
    """Main async function to run the feed summarizer once."""
    processor = NewsProcessor()
    try:
        await processor.initialize()
        
        # Run feed processing once
        await processor.process_all_feeds()
        
    except CancelledError:
        logger.info("Summarizer task was cancelled")
    except Exception as e:
        logger.error(f"Unexpected error in summarizer: {e}")
        logger.error(traceback.format_exc())
    finally:
        await processor.close()


