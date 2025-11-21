#!/usr/bin/env python3
"""
RSS Publisher for feed summaries.

This module generates RSS feeds containing bulletins for each summary group
defined in feeds.yaml. Each RSS item represents a bulletin containing summaries
published within a 4-hour time window, with AI-generated introductions.
"""

from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timezone, timedelta
from pathlib import Path
import yaml
import xml.etree.ElementTree as ET
from xml.dom import minidom
import os
import re
import tempfile
import shutil
from urllib.parse import urljoin
import json
from aiohttp import ClientSession, ClientError, ClientTimeout
from asyncio import sleep
from jinja2 import Environment, FileSystemLoader
from feedgen.feed import FeedGenerator
from bs4 import BeautifulSoup
from markdown import markdown as md

# Import configuration, models, and shared utilities
from config import config, get_logger
from telemetry import init_telemetry, get_tracer, trace_span
from llm_client import chat_completion as ai_chat_completion
from models import DatabaseQueue

# Module-specific logger
logger = get_logger("publisher")
init_telemetry("feed-summarizer-publisher")
_tracer = get_tracer("publisher")


# Azure storage uploader (imported conditionally)
try:
    from uploader import AzureStorageUploader
    AZURE_AVAILABLE = True
except ImportError:
    logger.debug("Azure uploader not available - Azure storage uploads disabled")
    AzureStorageUploader = None
    AZURE_AVAILABLE = False


# Initialize Jinja2 environment
env = Environment(loader=FileSystemLoader('templates'))


class RSSPublisher:
    """Publishes RSS feeds and HTML bulletins containing summary bulletins."""
    
    def __init__(self, base_url: str = "https://example.com", enable_azure_upload: bool = True):
        self.db: Optional[DatabaseQueue] = None
        # Ensure we generate content under the configured DATA_PATH/public
        self.public_dir = Path(config.PUBLIC_DIR)
        self.public_dir.mkdir(exist_ok=True)
        self.rss_feeds_dir = self.public_dir / "feeds"
        self.rss_feeds_dir.mkdir(exist_ok=True)
        self.html_bulletins_dir = self.public_dir / "bulletins"
        self.html_bulletins_dir.mkdir(exist_ok=True)
        # Replace hardcoded base_url with config.RSS_BASE_URL
        self.base_url = config.RSS_BASE_URL.rstrip('/')
        # Use retention from configuration thresholds (feeds.yaml) rather than hard-coded value
        try:
            self.retention_days = int(getattr(config, 'RETENTION_DAYS', 7))
        except Exception:
            self.retention_days = 7
        if self.retention_days < 1:
            logger.warning(f"Invalid RETENTION_DAYS={self.retention_days}; using fallback 7")
            self.retention_days = 7
        self.prompts = self._load_prompts()
        
        # Azure storage uploader (optional)
        self.azure_uploader = None
        self.enable_azure_upload = enable_azure_upload
        
    def _load_prompts(self) -> Dict[str, str]:
        """Load prompts from the prompt.yaml configuration file."""
        try:
            with open(config.PROMPT_CONFIG_PATH, 'r') as f:
                prompts_config = yaml.safe_load(f) or {}
                return prompts_config
        except Exception as e:
            logger.error(f"Error loading prompts from {config.PROMPT_CONFIG_PATH}: {e}")
            return {}

    def _load_passthrough_config(self) -> Dict[str, Any]:
        """Load passthrough configuration from feeds.yaml.

        Expected formats:
        passthrough:
          - slug1
          - slug2
        or
        passthrough:
          slug1: { limit: 50, title: "Custom" }
        """
        cfg = self._load_feeds_config()
        pt = cfg.get('passthrough', {})
        if isinstance(pt, list):
            # Convert to dict with defaults
            return {slug: {'limit': 50} for slug in pt}
        if isinstance(pt, dict):
            # Normalize entries
            normalized = {}
            for slug, opts in pt.items():
                if isinstance(opts, dict):
                    limit = int(opts.get('limit', 50))
                    title = opts.get('title')
                    normalized[slug] = {'limit': limit, 'title': title}
                else:
                    normalized[slug] = {'limit': 50}
            return normalized
        return {}
        
    async def initialize(self):
        """Initialize the publisher with database connection."""
        self.db = DatabaseQueue(config.DATABASE_PATH)
        await self.db.start()
        try:
            logger.info(f"Publisher paths: DATA_PATH={config.DATA_PATH} PUBLIC_DIR={config.PUBLIC_DIR} DB={config.DATABASE_PATH}")
        except Exception:
            pass
        
        # Initialize Azure uploader if enabled
        if self.enable_azure_upload and AZURE_AVAILABLE:
            self.azure_uploader = AzureStorageUploader()
            await self.azure_uploader.initialize()
        elif self.enable_azure_upload and not AZURE_AVAILABLE:
            logger.info("Azure upload requested but uploader module is unavailable; skipping Azure upload")
        
        logger.info("Unified Publisher initialized")

    async def close(self):
        """Close connections and clean up resources."""
        if self.db:
            await self.db.stop()
        if self.azure_uploader:
            await self.azure_uploader.close()
        logger.info("Unified Publisher closed")

    async def _get_latest_bulletin_title(self, group_name: str, days_back: int = 30) -> Optional[str]:
        """Return the most recent non-empty stored title for a group's bulletins."""
        try:
            bulletins = await self.db.execute('get_bulletins_for_group', group_name=group_name, days_back=days_back)
            if bulletins:
                for b in bulletins:
                    try:
                        t = (b.get('title') or '').strip() if isinstance(b, dict) else None
                    except Exception:
                        t = None
                    if t:
                        return t
        except Exception:
            return None
        return None

    # ---- Helper logic for landing page recent bulletins ----
    def _extract_bulletin_summary(self, bulletin_path: Path, max_len: int = 140) -> Optional[str]:
        """Extract a short summary snippet from a rendered bulletin HTML file.

        Strategy (cheap & resilient):
        1. Prefer introduction paragraph inside <div class="introduction">.
        2. Fallback to first <div class="summary-text"> block.
        3. Strip tags & collapse whitespace; truncate to max_len.
        """
        if not bulletin_path.exists():
            return None
        try:
            text = bulletin_path.read_text(encoding='utf-8', errors='ignore')
        except Exception:
            return None
        intro_match = re.search(r'<div class="introduction">.*?<p>(.*?)</p>', text, re.DOTALL | re.IGNORECASE)
        candidate = None
        if intro_match:
            candidate = intro_match.group(1)
        else:
            # First summary-text block
            summ_match = re.search(r'<div class="summary-text">(.*?)</div>', text, re.DOTALL | re.IGNORECASE)
            if summ_match:
                candidate = summ_match.group(1)
        if not candidate:
            return None
        # Remove any residual tags
        candidate = re.sub(r'<[^>]+>', ' ', candidate)
        candidate = re.sub(r'\s+', ' ', candidate).strip()
        if len(candidate) > max_len:
            candidate = candidate[: max_len - 1].rstrip() + '…'
        return candidate or None

    def build_recent_bulletins(self, latest_titles: Dict[str, Optional[str]]) -> List[Dict[str, str]]:
        """Build recent bulletin metadata list for landing page.

        Reads the corresponding HTML bulletin file to extract a short summary.
        Skips groups without a title. Returns list sorted by group name.
        """
        items: List[Dict[str, str]] = []
        for group_name, title in sorted(latest_titles.items()):
            if not title:
                continue
            bulletin_file = self.html_bulletins_dir / f"{group_name}.html"
            summary = self._extract_bulletin_summary(bulletin_file) or ''
            items.append({
                'filename': bulletin_file.name,
                'title': title,
                'summary': summary
            })
        return items

    # --- Consistent title retrieval for indexes ---
    def _extract_bulletin_file_title(self, group_name: str) -> Optional[str]:
        """Extract <h1> title from the rendered bulletin HTML file for a group.

        This ensures consistency between main index (which parses files) and
        feeds index (which previously used DB). Prefer file system truth over DB.
        """
        path = self.html_bulletins_dir / f"{group_name}.html"
        if not path.exists():
            return None
        try:
            text = path.read_text(encoding='utf-8', errors='ignore')
        except Exception:
            return None
        m = re.search(r'<h1[^>]*>(.*?)</h1>', text, re.IGNORECASE | re.DOTALL)
        if not m:
            return None
        title = re.sub(r'<[^>]+>', ' ', m.group(1))
        title = re.sub(r'\s+', ' ', title).strip()
        return title or None

    def _sanitize_xml_string(self, text: str) -> str:
        """Sanitize a string for XML output by removing control characters and NULL bytes."""
        if not text:
            return ''
        # Remove NULL bytes and control characters except tab, newline, and carriage return
        # Also ensure the string is valid UTF-8
        try:
            if isinstance(text, bytes):
                text = text.decode('utf-8', errors='ignore')
            # Remove control characters (0x00-0x1F) except tab (0x09), newline (0x0A), carriage return (0x0D)
            sanitized = ''.join(
                char for char in text 
                if char in ('\t', '\n', '\r') or (ord(char) >= 32 and ord(char) != 0x7F)
            )
            return sanitized
        except Exception:
            return ''
    
    def _create_raw_rss(self, slug: str, feed_title: str, items: List[Dict[str, Any]]) -> str:
        """Create an RSS feed for raw items of a specific feed slug (feedgen).
        
        Note: feedgen writes items in the order they are added via add_item().
        We want newest items first in the RSS output (reverse chronological).
        Items should be passed in oldest-first order so feedgen outputs them properly.
        """
        fg = FeedGenerator()
        fg.id(self._sanitize_xml_string(f"{self.base_url}/feeds/raw/{slug}.xml"))
        fg.title(self._sanitize_xml_string(feed_title or slug))
        fg.link(href=self._sanitize_xml_string(f"{self.base_url}/feeds/raw/{slug}.xml"), rel='self')
        fg.link(href=self._sanitize_xml_string(self.base_url), rel='alternate')
        fg.description(self._sanitize_xml_string(f"Passthrough feed for {slug}"))
        fg.language('en-us')
        fg.generator('Feed Summarizer Passthrough')
        fg.lastBuildDate(datetime.now(timezone.utc))

        for it in items:
            try:
                fe = fg.add_item()
                title = self._sanitize_xml_string(str(it.get('title') or it.get('url') or slug))
                link = self._sanitize_xml_string(str(it.get('url') or ''))
                guid_val = self._sanitize_xml_string(str(it.get('guid') or link or f"{slug}-{it.get('id')}"))
                fe.title(title if title else 'Untitled')
                if link:
                    fe.link(href=link)
                fe.guid(guid_val if guid_val else f"{slug}-{it.get('id')}", permalink=bool(guid_val and str(guid_val).startswith('http')))

                body = self._sanitize_xml_string(str(it.get('body') or '')).strip()
                if body:
                    short_plain = self._strip_markdown(body)
                    if len(short_plain) > 280:
                        short_plain = short_plain[:280].rsplit(' ', 1)[0] + "…"
                    # Sanitize the description again after markdown stripping
                    short_plain = self._sanitize_xml_string(short_plain)
                    fe.description(short_plain if short_plain else ' ')

                    needs_conversion = not self._looks_like_html(body) or self._looks_like_markdown(body)
                    html_body = body if not needs_conversion else self._markdown_to_html(body)
                    try:
                        soup = BeautifulSoup(html_body, 'html.parser')
                        for tag in soup.find_all(True):
                            for attr in ('href', 'src'):
                                if tag.has_attr(attr) and isinstance(tag[attr], str):
                                    cleaned = self._sanitize_xml_string(tag[attr].replace('\n', '').replace('\r', '').strip())
                                    # Ensure relative paths become absolute so images resolve when viewed out of context
                                    if link and cleaned and not cleaned.startswith(('http://', 'https://', 'data:')):
                                        cleaned = urljoin(link, cleaned)
                                    tag[attr] = cleaned
                        html_body = str(soup)
                    except Exception:
                        pass
                    # CRITICAL: Sanitize the final HTML content again before passing to feedgen
                    html_body = self._sanitize_xml_string(html_body)
                    fe.content(html_body if html_body else ' ', type='html')

                try:
                    dt = datetime.fromtimestamp(int(it.get('date', 0)), tz=timezone.utc)
                    fe.pubDate(dt)
                except Exception:
                    pass
            except Exception as e:
                logger.warning(f"Skipping problematic item {it.get('id')} in feed '{slug}': {e}")
                continue

        try:
            return fg.rss_str(pretty=True).decode('utf-8')
        except Exception as e:
            logger.error(f"Failed to generate RSS XML for feed '{slug}': {e}")
            # Try again without pretty printing
            try:
                return fg.rss_str(pretty=False).decode('utf-8')
            except Exception as e2:
                logger.error(f"Failed to generate RSS XML even without pretty printing for feed '{slug}': {e2}")
                raise

    def _looks_like_html(self, text: str) -> bool:
        """Heuristic: does the text already contain HTML tags?"""
        if '<' in text and '>' in text:
            for tag in ('<p', '<a ', '<br', '<ul', '<ol', '<li', '<em', '<strong', '<code', '<blockquote'):
                if tag in text:
                    return True
        return False

    def _looks_like_markdown(self, text: str) -> bool:
        """Detect common Markdown patterns to decide if conversion is needed."""
        if not text:
            return False
        markdown_patterns = (
            r"\[[^\]]+\]\([^\)]+\)",   # links
            r"!\[[^\]]*\]\([^\)]+\)",  # images
            r"(^|\n)#{1,6}\s+\S",        # headings
            r"(^|\n)(?:\*|-|\+)\s+\S", # unordered lists
            r"(^|\n)\d+\.\s+\S",       # ordered lists
            r"`[^`]+`",                    # inline code
            r"\*\*[^*]+\*\*",           # bold
            r"__[^_]+__",                  # bold alt
        )
        return any(re.search(pattern, text) for pattern in markdown_patterns)

    def _strip_markdown(self, text: str) -> str:
        """Remove basic Markdown syntax for a plain-text snippet."""
        t = text
        # Links: [text](url) -> text
        t = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", t)
        # Images: ![alt](url) -> alt
        t = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\1", t)
        # Bold/italic/code
        t = re.sub(r"\*\*([^*]+)\*\*", r"\1", t)
        t = re.sub(r"\*([^*]+)\*", r"\1", t)
        t = re.sub(r"`([^`]+)`", r"\1", t)
        # Headings and list markers
        t = re.sub(r"^\s{0,3}#{1,6}\s+", "", t, flags=re.MULTILINE)
        t = re.sub(r"^\s*[-*+]\s+", "• ", t, flags=re.MULTILINE)
        t = re.sub(r"^\s*\d+\.\s+", "• ", t, flags=re.MULTILINE)
        # Collapse whitespace
        t = re.sub(r"\s+", " ", t).strip()
        return t

    def _markdown_to_html(self, text: str) -> str:
        """Convert Markdown to HTML using the python-Markdown library."""
        # Use safe built-in extensions for basic formatting
        return md(text, extensions=['extra', 'sane_lists'])

    @trace_span("publish_passthrough_feeds", tracer_name="publisher")
    async def publish_passthrough_feeds(self, only_slugs: Optional[List[str]] = None) -> int:
        """Publish raw passthrough RSS feeds based on feeds.yaml passthrough config.

        Args:
            only_slugs: Optional list of slugs to limit publishing to.
        """
        try:
            pt = self._load_passthrough_config()
            if not pt:
                logger.info("No passthrough feeds configured")
                return 0

            # Ensure raw directory exists
            raw_dir = self.rss_feeds_dir / "raw"
            raw_dir.mkdir(parents=True, exist_ok=True)

            published = 0
            for slug, opts in pt.items():
                if only_slugs and slug not in set(only_slugs):
                    continue
                try:
                    # Resolve feed title
                    feed_meta = await self.db.execute('get_feed_by_slug', slug=slug)
                    feed_title = opts.get('title') or (feed_meta.get('title') if feed_meta else slug)
                    limit = int(opts.get('limit', 50))

                    items = await self.db.execute('query_latest_items_for_feed', slug=slug, limit=limit)
                    logger.info(f"Retrieved {len(items) if items else 0} items for passthrough feed '{slug}'")
                    if not items:
                        logger.info(f"No items found for passthrough feed '{slug}'")
                        continue

                    # Sort oldest-first, as feedgen outputs items in reverse order
                    items_sorted = sorted(
                        items,
                        key=lambda it: int(it.get('date') or 0),
                        reverse=False,
                    )

                    xml = self._create_raw_rss(slug, feed_title, items_sorted)
                    logger.info(f"Generated RSS XML for '{slug}' ({len(xml)} bytes)")

                    # Atomic write
                    out_file = raw_dir / f"{slug}.xml"
                    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.xml', dir=raw_dir, delete=False) as tf:
                        tf.write(xml)
                        tf.flush()
                        os.fsync(tf.fileno())
                        temp_path = tf.name
                    shutil.move(temp_path, out_file)
                    published += 1
                    logger.info(f"Published passthrough RSS: {out_file}")
                except Exception as fe:
                    logger.error(f"Error publishing passthrough feed '{slug}': {fe}", exc_info=True)
            return published
        except Exception as e:
            logger.error(f"Passthrough publishing failed: {e}")
            return 0

    def _load_feeds_config(self) -> Dict[str, Any]:
        """Load the feeds.yaml configuration."""
        try:
            with open("feeds.yaml", 'r') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"Error loading feeds.yaml: {e}")
            return {}

    @trace_span(
        "get_published_summaries_by_date",
        tracer_name="publisher",
        attr_from_args=lambda self, group_name, feed_slugs, days_back=7: {
            "group.name": group_name,
            "group.feed_count": len(feed_slugs or []),
            "days_back": int(days_back),
        },
    )
    async def get_published_summaries_by_date(self, group_name: str, feed_slugs: List[str], days_back: int = 7) -> Dict[str, List[Dict[str, Any]]]:
        """Get published summaries grouped by publication sessions.
        
        First checks for existing bulletins in the database, then falls back to 
        grouping summaries by session if no bulletins exist.
        """
        if not feed_slugs:
            return {}
        
        try:
            # First, try to get existing bulletins for this specific group
            logger.debug(f"Looking for cached bulletins for group '{group_name}'")
            
            bulletins_found = {}
            try:
                bulletins = await self.db.execute('get_bulletins_for_group', group_name=group_name, days_back=days_back)
                
                for bulletin_meta in bulletins or []:
                    session_key = bulletin_meta['session_key']
                    
                    # Get full bulletin data with summaries
                    bulletin_data = await self.db.execute('get_bulletin', 
                                                        group_name=group_name, 
                                                        session_key=session_key)
                    
                    if bulletin_data and bulletin_data.get('summaries'):
                        bulletins_found[session_key] = bulletin_data['summaries']
                        logger.debug(f"Found cached bulletin for {group_name}/{session_key} with {len(bulletin_data['summaries'])} summaries")
            
            except Exception as e:
                logger.warning(f"Error loading bulletins for group {group_name}: {e}")
            
            # If we found bulletins, return them
            if bulletins_found:
                logger.info(f"Using {len(bulletins_found)} cached bulletins for group '{group_name}'")
                return bulletins_found
            
            # Fallback: Generate bulletins from raw summaries (old behavior)
            logger.info(f"No cached bulletins found for group '{group_name}', generating from raw summaries for: {feed_slugs}")
            
            # Calculate cutoff time (days_back days ago)
            cutoff_time = int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp())
            
            summaries = await self.db.execute(
                'query_published_summaries_by_date', 
                feed_slugs=feed_slugs, 
                cutoff_time=cutoff_time
            )
            
            # Group summaries by exact publication timestamp (publishing session)
            grouped = {}
            for summary in summaries or []:
                if summary['published_date']:
                    # Convert timestamp to datetime
                    pub_date = datetime.fromtimestamp(summary['published_date'], tz=timezone.utc)
                    
                    # Create session key based on exact publication time (to nearest minute)
                    # This groups summaries published in the same batch/session
                    session_key = pub_date.strftime('%Y-%m-%d-%H-%M')
                    
                    if session_key not in grouped:
                        grouped[session_key] = []
                    grouped[session_key].append(summary)
            
            # If we have only one large session, split it into smaller bulletins
            # to avoid having hundreds of summaries in a single RSS item
            max_summaries_per_item = 25  # Maximum summaries per RSS item (increased for better content grouping)
            final_grouped = {}
            
            for session_key, session_summaries in grouped.items():
                if len(session_summaries) <= max_summaries_per_item:
                    # Small session, keep as is
                    final_grouped[session_key] = session_summaries
                else:
                    # Large session, split into multiple bulletins
                    for i in range(0, len(session_summaries), max_summaries_per_item):
                        chunk = session_summaries[i:i + max_summaries_per_item]
                        chunk_key = f"{session_key}-{i//max_summaries_per_item + 1}"
                        final_grouped[chunk_key] = chunk
            
            return final_grouped
            
        except Exception as e:
            logger.error(f"Error querying published summaries by date for group '{group_name}' with feeds {feed_slugs}: {e}")
            return {}

    async def get_latest_summaries_for_feeds(self, feed_slugs: List[str], limit: int = 50) -> List[Dict[str, Any]]:
        """Get the latest unpublished summaries for specified feeds (for HTML generation)."""
        if not feed_slugs:
            return []
        
        try:
            # Use the database method to query summaries
            summaries = await self.db.execute('query_summaries_for_feeds', feed_slugs=feed_slugs, limit=limit)
            return summaries or []
            
        except Exception as e:
            logger.error(f"Error querying summaries for feeds {feed_slugs}: {e}")
            return []

    async def mark_summaries_as_published(self, summary_ids: List[int]) -> int:
        """Mark summaries as published by updating the published_date in the database."""
        if not summary_ids:
            return 0
            
        try:
            # Use the database method to mark summaries as published
            marked_count = await self.db.execute('mark_summaries_as_published', summary_ids=summary_ids)
            logger.info(f"Marked {marked_count} summaries as published: {summary_ids}")
            return marked_count or 0
        except Exception as e:
            logger.error(f"Error marking summaries as published: {e}")
            return 0

    def _generate_markdown_bulletin(self, summaries: List[Dict[str, Any]]) -> str:
        """Generate a markdown bulletin grouped by topic for AI introduction generation.

        Summaries produced by the summarizer use keys like 'item_title' and 'item_url'. The
        shared markdown helpers expect 'title' and 'url'. Perform a lightweight projection
        to the expected shape to avoid KeyError 'title'.
        """
        projected: List[Dict[str, Any]] = []
        for s in summaries:
            try:
                projected.append({
                    'title': s.get('item_title') or s.get('title') or 'Untitled',
                    'url': s.get('item_url') or s.get('url') or '',
                    'topic': s.get('topic', 'General')
                })
            except Exception:
                # Skip any summary that can't be coerced
                continue
        topics: Dict[str, List[Dict[str, Any]]] = {}
        for entry in projected:
            topic = entry.get('topic', 'General')
            topics.setdefault(topic, []).append(entry)

        markdown_lines: List[str] = []
        for topic, items in topics.items():
            markdown_lines.append(f"\n## {topic}\n")
            for item in items:
                markdown_lines.append(f"- {item['title']} ({item['url']})")
        return "\n".join(markdown_lines)

    @trace_span(
        "publisher.azure_openai.intro",
        tracer_name="publisher",
        attr_from_args=lambda self, markdown_bulletin, session: {
            "prompt.length": len(markdown_bulletin or ""),
            "azure.openai.deployment": config.DEPLOYMENT_NAME,
            "azure.openai.api_version": config.OPENAI_API_VERSION,
        },
    )
    async def _generate_ai_introduction(self, markdown_bulletin: str, session: ClientSession) -> Optional[str]:
        """Generate an AI introduction using shared chat helper (post-processed to 1–2 concise sentences)."""
        intro_prompt = self.prompts.get('intro', '')
        if not intro_prompt:
            logger.error("No 'intro' prompt found in configuration")
            return None
        try:
            formatted_prompt = intro_prompt.format(body=markdown_bulletin)
        except Exception as e:
            logger.error(f"Error formatting intro prompt: {e}")
            return None

        def _postprocess(raw: str) -> str:
            text = " ".join(raw.split())
            parts = text.split('. ')
            if len(parts) >= 2:
                intro = parts[0].rstrip('. ') + '. ' + parts[1].rstrip('. ')
            else:
                intro = parts[0].rstrip('. ')
            words = intro.split()
            if len(words) > 60:
                intro = ' '.join(words[:60]).rstrip('.,;:! ') + '.'
            return intro

        messages = [{"role": "user", "content": formatted_prompt}]
        return await ai_chat_completion(
            messages,
            purpose="intro",
            postprocess=_postprocess,
        )

    @trace_span(
        "publisher.azure_openai.title",
        tracer_name="publisher",
        attr_from_args=lambda self, markdown_bulletin, session: {
            "prompt.length": len(markdown_bulletin or ""),
            "azure.openai.deployment": config.DEPLOYMENT_NAME,
            "azure.openai.api_version": config.OPENAI_API_VERSION,
        },
    )
    async def _generate_ai_title(self, markdown_bulletin: str, session: ClientSession) -> Optional[str]:
        """Generate a concise AI title for the bulletin using shared chat helper.

        Strategy:
        1. Send system + user prompt if system prompt is configured.
        2. If result empty after postprocess, retry once with a trimmed version (first 50 lines) to reduce context.
        3. Return None if still empty so caller can fallback.
        """
        title_prompt = self.prompts.get('title', '')
        if not title_prompt:
            logger.warning("No 'title' prompt found in configuration; cannot generate AI titles")
            return None
        try:
            formatted_prompt = title_prompt.format(body=markdown_bulletin)
        except Exception as e:
            logger.error(f"Error formatting title prompt: {e}")
            return None
        try:
            logger.debug(
                "Preparing AI title: bulletin_len=%d prompt_len=%d",
                len(markdown_bulletin or ''),
                len(formatted_prompt or ''),
            )
        except Exception:
            pass

        def _postprocess(raw: str) -> str:
            # Single line
            title = raw.splitlines()[0].strip()
            # Remove terminal punctuation we don't want in titles
            if title.endswith(('.', '!', '?', ':', ';')):
                title = title.rstrip('.!?:;').strip()
            # Hard cap word count
            words = title.split()
            if len(words) > 12:
                title = ' '.join(words[:12])
            if title:
                logger.info(f"Generated AI title: '{title[:120]}'")
            else:
                logger.warning("AI title generation returned an empty string after post-processing")
            return title

        # Include system prompt if present for better steering
        system_prompt = self.prompts.get('title_system') or self.prompts.get('system_title') or ''
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": formatted_prompt})
        result = await ai_chat_completion(messages, purpose="title", postprocess=_postprocess)
        if result:
            return result
        # Retry with trimmed context if initial attempt produced empty/None
        try:
            trimmed = "\n".join(markdown_bulletin.splitlines()[:50])
            if len(trimmed) < len(markdown_bulletin):
                logger.debug("Retrying AI title with trimmed bulletin context (%d -> %d chars)", len(markdown_bulletin), len(trimmed))
                retry_prompt = title_prompt.format(body=trimmed)
                retry_messages = []
                if system_prompt:
                    retry_messages.append({"role": "system", "content": system_prompt})
                retry_messages.append({"role": "user", "content": retry_prompt})
                result_retry = await ai_chat_completion(retry_messages, purpose="title_retry", postprocess=_postprocess)
                if result_retry:
                    return result_retry
        except Exception as e:
            logger.debug(f"AI title retry failed: {e}")
        return None

    def _generate_bulletin_content(self, summaries: List[Dict[str, Any]], introduction: Optional[str] = None) -> str:
        """Generate HTML content for a bulletin RSS item."""
        # Group summaries by topic
        topics = {}
        for summary in summaries:
            topic = summary.get('topic', 'General')
            if topic not in topics:
                topics[topic] = []
            topics[topic].append(summary)
        
        # Sort topics alphabetically and sort items within each topic by original publication date (newest first)
        sorted_topics = sorted(topics.keys())
        for topic in topics:
            topics[topic].sort(key=lambda x: x.get('item_date', 0), reverse=True)
        
        # Build HTML content
        content_parts = []
        
        # Add introduction if available
        if introduction and introduction.strip():
            content_parts.append(f"<p>{introduction.strip()}</p>")
        
        # Add topics and summaries
        for topic in sorted_topics:
            topic_summaries = topics[topic]
            content_parts.append(f"<h3>{topic}</h3>")
            content_parts.append("<ul>")
            
            for summary in topic_summaries:
                summary_text = summary.get('summary_text', '').strip()
                item_url = summary.get('item_url', '')
                
                if summary_text:
                    if item_url:
                        content_parts.append(f'<li>{summary_text} (<a href="{item_url}">link</a>)</li>')
                    else:
                        content_parts.append(f'<li>{summary_text}</li>')
            
            content_parts.append("</ul>")
        
        return "\n".join(content_parts)

    def _generate_title_from_introduction(self, introduction: str, group_name: str, session_key: str) -> str:
        """Generate a descriptive title from the AI introduction."""
        if not introduction or not introduction.strip():
            # Fallback to session-based title if no introduction
            try:
                # Handle both old format (YYYY-MM-DD-HH) and new format (YYYY-MM-DD-HH-MM)
                if session_key.count('-') == 4:  # New format: YYYY-MM-DD-HH-MM
                    bulletin_time = datetime.strptime(session_key[:16], '%Y-%m-%d-%H-%M').replace(tzinfo=timezone.utc)
                elif session_key.count('-') >= 5:  # Chunked format: YYYY-MM-DD-HH-MM-N
                    base_time_str = '-'.join(session_key.split('-')[:5])  # Take first 5 parts
                    bulletin_time = datetime.strptime(base_time_str, '%Y-%m-%d-%H-%M').replace(tzinfo=timezone.utc)
                    chunk_number = session_key.split('-')[-1]
                    return f"{group_name.title()} Bulletin #{chunk_number} - {bulletin_time.strftime('%Y-%m-%d %H:%M UTC')}"
                else:  # Old format: YYYY-MM-DD-HH (backward compatibility)
                    bulletin_time = datetime.strptime(session_key, '%Y-%m-%d-%H').replace(tzinfo=timezone.utc)
                
                date_str = bulletin_time.strftime('%Y-%m-%d')
                time_str = bulletin_time.strftime('%H:%M')
                return f"{group_name.title()} Bulletin - {date_str} {time_str} UTC"
            except ValueError:
                # If parsing fails, use a generic title
                return f"{group_name.title()} News Bulletin"
        
        # Extract key themes from introduction (first sentence or first ~50 characters)
        intro_clean = introduction.strip()
        
        # Try to get the first sentence, but limit length
        first_sentence = intro_clean.split('.')[0]
        if len(first_sentence) > 80:
            # If first sentence is too long, take first ~50 chars and find a good break point
            short_intro = intro_clean[:50]
            last_space = short_intro.rfind(' ')
            if last_space > 20:  # Make sure we don't cut too short
                short_intro = short_intro[:last_space]
            title_base = short_intro
        else:
            title_base = first_sentence
        
        # Clean up and format the title
        title_base = title_base.replace('\n', ' ').strip()
        if not title_base.endswith('.'):
            title_base += "..."
        
        return f"{title_base}"

    def _create_rss_feed(self, group_name: str, feed_slugs: List[str], bulletins: Dict[str, List[Dict[str, Any]]], bulletin_introductions: Dict[str, str] = None, bulletin_titles: Dict[str, str] = None) -> str:
        """Create RSS 2.0 XML content for a summary group (feedgen)."""
        fg = FeedGenerator()
        feed_url = f"{self.base_url}/feeds/{group_name}.xml"
        fg.id(feed_url)
        fg.title(f"{group_name.title()} News Bulletins")
        fg.link(href=feed_url, rel='self')
        fg.link(href=self.base_url, rel='alternate')
        fg.description(f"News bulletins for {group_name} topics, featuring AI-generated summaries from multiple sources, updated every 4 hours.")
        fg.language('en-us')
        fg.generator('Feed Summarizer RSS Publisher')
        fg.lastBuildDate(datetime.now(timezone.utc))

        # Items per bulletin session
        for session_key in sorted(bulletins.keys(), reverse=True):
            summaries = bulletins[session_key]
            if not summaries:
                continue

            fe = fg.add_item()

            introduction = bulletin_introductions.get(session_key) if bulletin_introductions else None
            if bulletin_titles and session_key in bulletin_titles and bulletin_titles[session_key]:
                title_text = bulletin_titles[session_key]
            else:
                title_text = self._generate_title_from_introduction(introduction, group_name, session_key)
            fe.title(title_text)

            fe.link(href=f"{self.base_url}/bulletins/{group_name}.html")
            fe.guid(f"{group_name}-bulletin-{session_key}", permalink=False)

            # Description: short plain text (prefer AI intro)
            desc_plain = (introduction or f"Bulletin for {group_name}").strip()
            if len(desc_plain) > 280:
                desc_plain = desc_plain[:280].rsplit(' ', 1)[0] + "…"
            fe.description(desc_plain)

            # content:encoded with full HTML bulletin
            html_body = self._generate_bulletin_content(summaries, introduction)
            fe.content(html_body, type='html')

            # pubDate from latest summary
            try:
                latest_pub_date = max(s['published_date'] for s in summaries if s.get('published_date'))
                fe.pubDate(datetime.fromtimestamp(latest_pub_date, tz=timezone.utc))
            except Exception:
                pass

        return fg.rss_str(pretty=True).decode('utf-8')

    @trace_span(
        "publish_rss_feed",
        tracer_name="publisher",
        attr_from_args=lambda self, group_name, feed_slugs: {"group.name": group_name, "group.feed_count": len(feed_slugs or [])},
    )
    async def publish_rss_feed(self, group_name: str, feed_slugs: List[str], enable_intro: bool = False) -> bool:
        """Publish an RSS feed for a summary group."""
        try:
            logger.info(f"Publishing RSS feed for group '{group_name}' with feeds: {feed_slugs}")
            
            # Get published summaries grouped by publishing sessions (last 7 days for processing, but we'll filter to retention period)
            bulletins = await self.get_published_summaries_by_date(group_name, feed_slugs, days_back=7)
            
            # Filter bulletins to retention period
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
            filtered_bulletins = {}
            for session_key, summaries in bulletins.items():
                try:
                    # Parse the session key to get the publication time
                    # Handle both session format (YYYY-MM-DD-HH-MM) and chunked format (YYYY-MM-DD-HH-MM-N)
                    if session_key.count('-') >= 4:
                        # Extract time part (first 5 components: YYYY-MM-DD-HH-MM)
                        time_parts = session_key.split('-')[:5]
                        time_str = '-'.join(time_parts)
                        bulletin_time = datetime.strptime(time_str, '%Y-%m-%d-%H-%M').replace(tzinfo=timezone.utc)
                    else:
                        # Fallback for old format compatibility
                        bulletin_time = datetime.strptime(session_key, '%Y-%m-%d-%H').replace(tzinfo=timezone.utc)
                        
                    if bulletin_time >= cutoff_date:
                        filtered_bulletins[session_key] = summaries
                except ValueError as e:
                    logger.warning(f"Could not parse session key '{session_key}': {e}")
                    # Include it anyway to be safe
                    filtered_bulletins[session_key] = summaries
            
            if not filtered_bulletins:
                logger.info(f"No recent bulletins found for group '{group_name}' within {self.retention_days} days")
                return True  # Not an error, just no content
            
            # Get AI introductions/titles for each bulletin
            # First, check if we have cached introductions and titles from the bulletins table
            bulletin_introductions = {}
            bulletin_titles: Dict[str, str] = {}
            
            # Try to get cached introductions first (only if intro enabled)
            for session_key, summaries in filtered_bulletins.items():
                try:
                    bulletin_data = await self.db.execute('get_bulletin', 
                                                        group_name=group_name, 
                                                        session_key=session_key)
                    if enable_intro and bulletin_data and bulletin_data.get('introduction'):
                        bulletin_introductions[session_key] = bulletin_data['introduction']
                        logger.debug(f"Using cached introduction for '{group_name}' bulletin {session_key}")
                    if bulletin_data and bulletin_data.get('title'):
                        t = (bulletin_data.get('title') or '').strip()
                        if t:
                            bulletin_titles[session_key] = t
                            logger.debug(f"Using cached title for '{group_name}' bulletin {session_key}: '{t[:80]}'")
                except Exception as e:
                    logger.debug(f"No cached bulletin data for '{group_name}' {session_key}: {e}")
            
            # Generate AI introductions for bulletins that don't have cached ones
            missing_introductions = set(filtered_bulletins.keys()) - set(bulletin_introductions.keys())
            
            if enable_intro and missing_introductions and config.AZURE_ENDPOINT and config.OPENAI_API_KEY:
                async with ClientSession() as session:
                    for session_key in missing_introductions:
                        summaries = filtered_bulletins[session_key]
                        try:
                            markdown_bulletin = self._generate_markdown_bulletin(summaries)
                            introduction = await self._generate_ai_introduction(markdown_bulletin, session)
                            if introduction:
                                bulletin_introductions[session_key] = introduction
                                logger.info(f"Generated AI introduction for '{group_name}' bulletin {session_key} ({len(introduction)} characters)")
                                
                                # Cache the introduction in the database for future use
                                try:
                                    summary_ids = [s['id'] for s in summaries]
                                    await self.db.execute('create_bulletin',
                                                        group_name=group_name,
                                                        session_key=session_key,
                                                        introduction=introduction,
                                                        summary_ids=summary_ids,
                                                        feed_slugs=feed_slugs)
                                    logger.debug(f"Cached introduction for future use: '{group_name}' session '{session_key}'")
                                except Exception as cache_error:
                                    logger.warning(f"Failed to cache introduction: {cache_error}")
                            else:
                                logger.warning(f"Failed to generate AI introduction for '{group_name}' bulletin {session_key}")
                        except Exception as e:
                            logger.error(f"Error generating AI introduction for '{group_name}' bulletin {session_key}: {e}")
            
            # Log cache efficiency
            if enable_intro:
                cached_count = len(bulletin_introductions)
                if cached_count > 0:
                    logger.info(f"Using introductions for '{group_name}': {cached_count}/{len(filtered_bulletins)}")
            
            # Titles now expected to be persisted during HTML bulletin creation.
            # Reuse cached titles; if missing, derive a fallback from introduction/session.
            for skey in filtered_bulletins.keys():
                if skey not in bulletin_titles:
                    intro = bulletin_introductions.get(skey)
                    fallback_title = self._generate_title_from_introduction(intro or "", group_name, skey)
                    bulletin_titles[skey] = fallback_title
                    # Optional backfill (non-AI) only if title column empty
                    try:
                        await self.db.execute('update_bulletin_title', group_name=group_name, session_key=skey, title=fallback_title)
                    except Exception:
                        pass

            # Summary of title generation coverage
            try:
                logger.info(f"AI titles generated for '{group_name}': {len(bulletin_titles)}/{len(filtered_bulletins)}")
            except Exception:
                pass

            # Generate RSS content (with introductions and optional titles)
            rss_content = self._create_rss_feed(group_name, feed_slugs, filtered_bulletins, bulletin_introductions if enable_intro else {}, bulletin_titles)
            
            # Write to file atomically
            output_file = self.rss_feeds_dir / f"{group_name}.xml"
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.xml', 
                                           dir=self.rss_feeds_dir, delete=False) as temp_file:
                temp_file.write(rss_content)
                temp_file.flush()
                os.fsync(temp_file.fileno())
                temp_path = temp_file.name
            
            # Atomic move
            shutil.move(temp_path, output_file)
            
            total_items = len(filtered_bulletins)
            logger.info(f"Successfully published RSS feed with {total_items} bulletin(s) to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error publishing RSS feed for group '{group_name}': {e}")
            return False

    async def _write_index_html(self):
        """Generate index.html for RSS feeds directory."""
        feeds_index_path = self.rss_feeds_dir / "index.html"
        
        # Get list of summary RSS feeds and passthrough feeds
        rss_files = list(self.rss_feeds_dir.glob("*.xml"))
        raw_dir = self.rss_feeds_dir / "raw"
        raw_files = list(raw_dir.glob("*.xml")) if raw_dir.exists() else []
        if not rss_files:
            logger.warning("No RSS feeds found for index generation")
            # Still create an index for passthrough files if present
            if not raw_files:
                return
            
        # Load feeds config to get titles and descriptions
        config_data = self._load_feeds_config()
        summaries_config = config_data.get('summaries', {})

        feeds_info = []
        for rss_file in sorted(rss_files):
            group_name = rss_file.stem
            # Check if group_config is available and is a dict, otherwise use defaults
            if isinstance(summaries_config.get(group_name), dict):
                group_config = summaries_config[group_name]
            else:
                group_config = {}
            # Skip hidden feeds from HTML listing
            try:
                hidden = False
                if group_config:
                    hv = group_config.get('hidden')
                    if isinstance(hv, str):
                        hidden = hv.strip().lower() == 'true'
                    elif isinstance(hv, bool):
                        hidden = hv
                    else:
                        hidden = False
                    # Support alternate flags
                    if group_config.get('visible') is False:
                        hidden = True
                    if group_config.get('hide_from_index') is True:
                        hidden = True
                if hidden:
                    logger.info(f"Hiding '{group_name}' from RSS feeds HTML index due to config flag")
                    continue
            except Exception:
                pass
            # Fetch latest stored bulletin title (if any) for this group to surface on index
            latest_title = await self._get_latest_bulletin_title(group_name, days_back=30)
            file_title = self._extract_bulletin_file_title(group_name)
            if file_title:
                latest_title = file_title  # file system truth wins

            feeds_info.append({
                'name': group_name,
                'title': group_config.get('title', group_name.replace('_', ' ').title()),
                'description': group_config.get('description', f'{group_name} news summaries'),
                'filename': rss_file.name,
                'latest_title': latest_title
            })

        # Build passthrough list (respect hidden flags on individual feeds)
        pt_cfg = self._load_passthrough_config()
        feeds_config = config_data.get('feeds', {}) if isinstance(config_data, dict) else {}
        passthrough_info = []
        if raw_files and pt_cfg:
            for rf in sorted(raw_files):
                slug = rf.stem
                # Only expose passthrough feeds explicitly configured
                if slug not in pt_cfg:
                    continue
                opts = pt_cfg.get(slug, {}) or {}
                # Determine hidden flags (support string/bool forms and aliases)
                hidden = False
                try:
                    hv = opts.get('hidden')
                    if isinstance(hv, str):
                        hidden = hv.strip().lower() == 'true'
                    elif isinstance(hv, bool):
                        hidden = hv
                    # If not explicitly set in passthrough options, consult feed-level config
                    if not hidden and isinstance(feeds_config.get(slug), dict):
                        feed_cfg = feeds_config.get(slug) or {}
                        fhv = feed_cfg.get('hidden')
                        if isinstance(fhv, str):
                            hidden = fhv.strip().lower() == 'true'
                        elif isinstance(fhv, bool):
                            hidden = fhv
                    if opts.get('visible') is False:
                        hidden = True
                    if opts.get('hide_from_index') is True:
                        hidden = True
                    # Feed-level aliases for completeness
                    if isinstance(feeds_config.get(slug), dict):
                        feed_cfg = feeds_config.get(slug) or {}
                        if feed_cfg.get('visible') is False:
                            hidden = True
                        if feed_cfg.get('hide_from_index') is True:
                            hidden = True
                except Exception:
                    pass
                if hidden:
                    logger.info(f"Hiding passthrough feed '{slug}' from RSS index due to config flag")
                    continue
                passthrough_info.append({
                    'name': slug,
                    'title': opts.get('title') or (feeds_config.get(slug, {}) or {}).get('title') or slug.replace('_', ' ').title(),
                    'filename': f"raw/{rf.name}"
                })
        
        # Use the feeds_index.html template (dynamic rendering)
        template = env.get_template('feeds_index.html')
        current_time = datetime.now(timezone.utc)
        html_content = template.render(
            feeds_info=feeds_info,
            passthrough_info=passthrough_info,
            current_time=current_time,
            base_url=self.base_url
        )

        with open(feeds_index_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Generated RSS feeds index: {feeds_index_path}")

    async def _write_bulletins_index_html(self):
        """Generate index.html for HTML bulletins directory."""
        bulletins_index_path = self.html_bulletins_dir / "index.html"
        
        # Get list of HTML bulletins
        html_files = list(self.html_bulletins_dir.glob("*.html"))
        html_files = [f for f in html_files if f.name != "index.html"]
        
        if not html_files:
            logger.warning("No HTML bulletins found for index generation")
            return
        
        # Load feeds config to get titles and descriptions
        config_data = self._load_feeds_config()
        summaries_config = config_data.get('summaries', {})

        bulletins_info = []
        for html_file in sorted(html_files, reverse=True):  # Most recent first
            group_name = html_file.stem
            # Check if group_config is available and is a dict, otherwise use defaults
            if isinstance(summaries_config.get(group_name), dict):
                group_config = summaries_config[group_name]
            else:
                group_config = {}
            # Skip hidden groups from HTML bulletins index
            try:
                hidden = False
                if group_config:
                    hv = group_config.get('hidden')
                    if isinstance(hv, str):
                        hidden = hv.strip().lower() == 'true'
                    elif isinstance(hv, bool):
                        hidden = hv
                    else:
                        hidden = False
                    if group_config.get('visible') is False:
                        hidden = True
                    if group_config.get('hide_from_index') is True:
                        hidden = True
                if hidden:
                    logger.info(f"Hiding '{group_name}' from HTML bulletins index due to config flag")
                    continue
            except Exception:
                pass
            
            # Resolve latest stored bulletin title for this group (if any)
            latest_title = await self._get_latest_bulletin_title(group_name, days_back=30)

            # Get file modification time
            mtime = datetime.fromtimestamp(html_file.stat().st_mtime, tz=timezone.utc)
            
            bulletins_info.append({
                'name': group_name,
                'title': group_config.get('title', group_name.replace('_', ' ').title()),
                'description': group_config.get('description', f'{group_name} news summaries'),
                'filename': html_file.name,
                'updated': mtime.strftime("%Y-%m-%d %H:%M UTC"),
                'latest_title': latest_title
            })
        
        # Use the bulletins_index.html template
        template = env.get_template('bulletins_index.html')
        current_time = datetime.now(timezone.utc)
        html_content = template.render(
            bulletins_info=bulletins_info,
            current_time=current_time,
            base_url=self.base_url
        )

        with open(bulletins_index_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Generated HTML bulletins index: {bulletins_index_path}")

    async def _write_main_index_html(self):
        """Generate main index.html for the public directory."""
        main_index_path = self.public_dir / "index.html"
        
        # Load feeds config to get summary counts
        config_data = self._load_feeds_config()
        summaries_config = config_data.get('summaries', {})
        
        # Count bulletins and feeds
        bulletins_count = len([f for f in self.html_bulletins_dir.glob("*.html") if f.name != "index.html"])
        feeds_count = len(list(self.rss_feeds_dir.glob("*.xml")))

        # Gather latest bulletin titles per group to surface on landing page
        latest_titles: Dict[str, Optional[str]] = {}
        try:
            if isinstance(summaries_config, dict):
                for group_name in summaries_config.keys():
                    latest_titles[group_name] = await self._get_latest_bulletin_title(group_name, days_back=30)
        except Exception:
            pass
        
        # Prepare recent bulletins: enumerate actual bulletin HTML files directly to avoid DB gaps
        recent_bulletins = []
        for html_file in sorted(self.html_bulletins_dir.glob('*.html')):
            if html_file.name == 'index.html':
                continue
            try:
                text = html_file.read_text(encoding='utf-8', errors='ignore')
            except Exception:
                text = ''
            title_match = re.search(r'<h1[^>]*>(.*?)</h1>', text, re.IGNORECASE | re.DOTALL)
            if title_match:
                raw_title = re.sub(r'<[^>]+>', ' ', title_match.group(1))
                raw_title = re.sub(r'\s+', ' ', raw_title).strip()
            else:
                stem = html_file.stem
                raw_title = latest_titles.get(stem) or stem.replace('_', ' ').title()
            summary = self._extract_bulletin_summary(html_file) or ''
            recent_bulletins.append({
                'filename': html_file.name,
                'title': raw_title,
                'summary': summary
            })

        # Use the index.html template
        template = env.get_template('index.html')
        current_time = datetime.now(timezone.utc)
        html_content = template.render(
            bulletins_count=bulletins_count,
            feeds_count=feeds_count,
            current_time=current_time,
            recent_bulletins=recent_bulletins,
            base_url=self.base_url
        )

        with open(main_index_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Generated main index: {main_index_path}")

    async def cleanup_old_bulletins(self, days_to_keep: int = None) -> int:
        """Clean up old bulletins from the database.
        
        Args:
            days_to_keep: Number of days to keep. Defaults to retention_days + 1
            
        Returns:
            Number of bulletins deleted
        """
        if days_to_keep is None:
            days_to_keep = self.retention_days + 1  # Keep a bit longer than RSS retention
        
        try:
            deleted_count = await self.db.execute('delete_old_bulletins', days_to_keep=days_to_keep)
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old bulletins (older than {days_to_keep} days)")
            return deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up old bulletins: {e}")
            return 0

    @trace_span(
        "publish_html_bulletin",
        tracer_name="publisher",
        attr_from_args=lambda self, group_name, feed_slugs: {"group.name": group_name, "group.feed_count": len(feed_slugs or [])},
    )
    async def publish_html_bulletin(self, group_name: str, feed_slugs: List[str], enable_intro: bool = False) -> bool:
        """Publish an HTML bulletin for a summary group."""
        try:
            logger.info(f"Publishing HTML bulletin for group '{group_name}' with feeds: {feed_slugs}")
            
            # Get latest summaries for the feeds
            summaries = await self.get_latest_summaries_for_feeds(feed_slugs, limit=100)

            # Normalize date fields to datetime objects for template safety
            if summaries:
                for s in summaries:
                    try:
                        d = s.get('item_date')
                        if d:
                            # Already a datetime?
                            if hasattr(d, 'strftime'):
                                pass
                            elif isinstance(d, (int, float)):
                                s['item_date'] = datetime.fromtimestamp(int(d), tz=timezone.utc)
                            elif isinstance(d, str):
                                # Try common formats
                                ds = d.strip()
                                # Handle trailing Z
                                if ds.endswith('Z'):
                                    ds_try = ds[:-1]
                                else:
                                    ds_try = ds
                                parsed = None
                                for fmt in ('%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S'):
                                    try:
                                        parsed = datetime.strptime(ds_try, fmt).replace(tzinfo=timezone.utc)
                                        break
                                    except Exception:
                                        continue
                                if not parsed:
                                    # Fallback: fromisoformat (Python 3.11+ handles offsets) inside try
                                    try:
                                        parsed = datetime.fromisoformat(ds_try)
                                        if parsed.tzinfo is None:
                                            parsed = parsed.replace(tzinfo=timezone.utc)
                                    except Exception:
                                        parsed = None
                                if parsed:
                                    s['item_date'] = parsed
                                else:
                                    # Drop invalid date to avoid template errors
                                    s['item_date'] = None
                    except Exception:
                        # On any normalization error, null out date to keep rendering robust
                        try:
                            s['item_date'] = None
                        except Exception:
                            pass
            
            if not summaries:
                logger.info(f"No unpublished summaries found for group '{group_name}'")
                return True  # Not an error, just no content
            
            # Diagnostics: log distribution by topic and by feed
            try:
                by_topic = {}
                by_feed = {}
                for s in summaries:
                    t = s.get('topic') or 'General'
                    by_topic[t] = by_topic.get(t, 0) + 1
                    f = s.get('feed_slug') or ''
                    if f:
                        by_feed[f] = by_feed.get(f, 0) + 1
                topic_info = ", ".join([f"{k}:{v}" for k, v in sorted(by_topic.items())])
                feed_info = ", ".join([f"{k}:{v}" for k, v in sorted(by_feed.items())])
                logger.info(f"Bulletin '{group_name}' will include {len(summaries)} summaries across {len(by_topic)} topic(s): {topic_info}")
                if by_feed:
                    logger.debug(f"Bulletin '{group_name}' feed distribution: {feed_info}")
                if len(by_topic) == 1 and len(summaries) <= 2:
                    logger.warning(f"Bulletin '{group_name}' appears small (topics={len(by_topic)}, items={len(summaries)}). Check summarizer logs for content filtering or limited new items.")
            except Exception:
                pass
            
            # Generate AI introduction and title if configured
            introduction: Optional[str] = None
            ai_title: Optional[str] = None
            if enable_intro and config.AZURE_ENDPOINT and config.OPENAI_API_KEY:
                try:
                    markdown_bulletin = self._generate_markdown_bulletin(summaries)
                    async with ClientSession() as session:
                        # Introduction
                        introduction = await self._generate_ai_introduction(markdown_bulletin, session)
                        if introduction:
                            logger.info(f"Generated AI introduction for '{group_name}' ({len(introduction)} characters)")
                        else:
                            logger.warning(f"Failed to generate AI introduction for '{group_name}'")
                        # Title
                        ai_title = await self._generate_ai_title(markdown_bulletin, session)
                        if ai_title:
                            logger.info(f"Generated AI title for '{group_name}': '{ai_title[:120]}'")
                        else:
                            logger.warning(f"Primary AI title attempt empty for '{group_name}' with intro enabled; will perform alternative title attempt")
                            # Alternative condensed attempt using top item titles
                            try:
                                condensed_titles = [ (s.get('item_title') or s.get('title','')).strip() for s in summaries if (s.get('item_title') or s.get('title')) ]
                                condensed = "\n".join(condensed_titles[:8])
                                if condensed:
                                    alt_prompt = f"Generate a concise bulletin title summarizing these article titles:\n{condensed}"
                                    async with ClientSession() as session_alt:
                                        alt_messages = []
                                        system_prompt = self.prompts.get('title_system') or self.prompts.get('system_title') or ''
                                        if system_prompt:
                                            alt_messages.append({"role": "system", "content": system_prompt})
                                        alt_messages.append({"role": "user", "content": alt_prompt})
                                        alt_title = await ai_chat_completion(alt_messages, purpose="title_alt", postprocess=lambda r: r.splitlines()[0].strip())
                                        if alt_title:
                                            ai_title = alt_title
                                            logger.info(f"Alternative AI title for '{group_name}': '{ai_title[:120]}'")
                                else:
                                    logger.debug(f"No condensed titles available for alternative AI title attempt '{group_name}'")
                            except Exception as e:
                                logger.debug(f"Alternative AI title attempt failed for '{group_name}': {e}")
                except Exception as e:
                    logger.error(f"Error generating AI intro/title for '{group_name}': {e}")
            else:
                # Even if intro disabled, title may still be generated if Azure is configured
                if config.AZURE_ENDPOINT and config.OPENAI_API_KEY:
                    try:
                        markdown_bulletin = self._generate_markdown_bulletin(summaries)
                        async with ClientSession() as session:
                            ai_title = await self._generate_ai_title(markdown_bulletin, session)
                            if ai_title:
                                logger.info(f"Generated AI title for '{group_name}': '{ai_title[:120]}'")
                            else:
                                logger.warning(f"Primary AI title attempt empty for '{group_name}' (intro disabled); performing alternative title attempt")
                                try:
                                    condensed_titles = [ (s.get('item_title') or s.get('title','')).strip() for s in summaries if (s.get('item_title') or s.get('title')) ]
                                    condensed = "\n".join(condensed_titles[:8])
                                    if condensed:
                                        alt_prompt = f"Generate a concise bulletin title summarizing these article titles:\n{condensed}"
                                        async with ClientSession() as session_alt:
                                            alt_messages = []
                                            system_prompt = self.prompts.get('title_system') or self.prompts.get('system_title') or ''
                                            if system_prompt:
                                                alt_messages.append({"role": "system", "content": system_prompt})
                                            alt_messages.append({"role": "user", "content": alt_prompt})
                                            alt_title = await ai_chat_completion(alt_messages, purpose="title_alt", postprocess=lambda r: r.splitlines()[0].strip())
                                            if alt_title:
                                                ai_title = alt_title
                                                logger.info(f"Alternative AI title for '{group_name}': '{ai_title[:120]}'")
                                    else:
                                        logger.debug(f"No condensed titles available for alternative AI title attempt '{group_name}' (intro disabled)")
                                except Exception as e:
                                    logger.debug(f"Alternative AI title attempt failed for '{group_name}' (intro disabled): {e}")
                    except Exception as e:
                        logger.error(f"Error generating AI title for '{group_name}': {e}")
            
            # Generate HTML content
            # Establish a unified final title BEFORE rendering HTML so RSS and HTML match.
            # Reuse existing heuristic cascade (AI title -> concatenated item titles -> intro-derived fallback).
            final_title = ai_title
            if not final_title:
                try:
                    concat_titles = [ (s.get('item_title') or s.get('title','')).strip() for s in summaries if (s.get('item_title') or s.get('title')) ]
                    if concat_titles:
                        heuristic = ", ".join(concat_titles[:5])[:120]
                        final_title = f"{group_name.title()}: {heuristic}".rstrip(' ,')
                except Exception:
                    pass
                if not final_title:
                    try:
                        # Use a provisional session key based on current time for fallback title generation
                        provisional_session_key = datetime.now(timezone.utc).strftime('%Y-%m-%d-%H-%M')
                        final_title = self._generate_title_from_introduction(introduction or "", group_name, provisional_session_key)
                    except Exception:
                        final_title = f"{group_name.title()} Bulletin"

            html_content = self._generate_html_content(group_name, feed_slugs, summaries, introduction, final_title)
            
            # Write to file atomically
            output_file = self.html_bulletins_dir / f"{group_name}.html"
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.html', 
                                           dir=self.html_bulletins_dir, delete=False) as temp_file:
                temp_file.write(html_content)
                temp_file.flush()
                os.fsync(temp_file.fileno())
                temp_path = temp_file.name
            
            # Atomic move
            shutil.move(temp_path, output_file)
            
            # Check if bulletins already exist for these summaries
            summary_ids = [s['id'] for s in summaries]
            
            # Check if any of these summaries are already in bulletins
            existing_bulletins = await self._check_existing_bulletins_for_summaries(group_name, summary_ids)
            
            if existing_bulletins:
                logger.info(f"Summaries for group '{group_name}' already exist in {len(existing_bulletins)} bulletins - skipping creation")
                return True
            
            # Mark summaries as published and get the actual published timestamp
            published_count = await self.mark_summaries_as_published(summary_ids)
            
            # Create bulletin record in database for caching (only if not already existing)
            if summary_ids and published_count > 0:
                try:
                    # Generate session key based on the actual publication timestamp (now that they're marked)
                    # Use a consistent timestamp for all summaries in this batch
                    current_time = datetime.now(timezone.utc)
                    session_key = current_time.strftime('%Y-%m-%d-%H-%M')
                    
                    # If we have many summaries, create multiple bulletins
                    max_summaries_per_bulletin = 25  # Increased to reduce unnecessary chunking
                    if len(summary_ids) <= max_summaries_per_bulletin:
                        # Generate AI title already attempted above (ai_title). Fallback to intro/session-derived title if missing.
                        # final_title already determined pre-render; reuse it to ensure consistency.
                        # (If heuristics differ due to time, prefer previously computed final_title.)
                        await self.db.execute('create_bulletin',
                                               group_name=group_name,
                                               session_key=session_key,
                                               introduction=introduction or "",  # Empty string if no introduction
                                               summary_ids=summary_ids,
                                               feed_slugs=feed_slugs,
                                               title=final_title)
                        logger.info(f"Created bulletin record for '{group_name}' session '{session_key}' with {len(summary_ids)} summaries (title='{final_title[:80] if final_title else ''}')")
                    else:
                        # Multiple bulletins - generate separate introductions for each chunk
                        for i in range(0, len(summary_ids), max_summaries_per_bulletin):
                            chunk_ids = summary_ids[i:i + max_summaries_per_bulletin]
                            chunk_key = f"{session_key}-{i//max_summaries_per_bulletin + 1}"
                            
                            # Get the actual summaries for this chunk to generate a specific introduction
                            chunk_summaries = [s for s in summaries if s['id'] in chunk_ids]
                            
                            # Generate a specific introduction for this chunk (only if enabled)
                            chunk_intro = ""
                            if enable_intro and config.AZURE_ENDPOINT and config.OPENAI_API_KEY and chunk_summaries:
                                try:
                                    chunk_markdown = self._generate_markdown_bulletin(chunk_summaries)
                                    async with ClientSession() as session:
                                        chunk_intro = await self._generate_ai_introduction(chunk_markdown, session)
                                        if chunk_intro:
                                            logger.info(f"Generated specific AI introduction for '{group_name}' chunk {chunk_key} ({len(chunk_intro)} characters)")
                                        else:
                                            logger.warning(f"Failed to generate AI introduction for '{group_name}' chunk {chunk_key}")
                                except Exception as e:
                                    logger.error(f"Error generating AI introduction for '{group_name}' chunk {chunk_key}: {e}")
                            
                            # Attempt AI title for this chunk; fallback if unavailable.
                            chunk_title = None
                            if config.AZURE_ENDPOINT and config.OPENAI_API_KEY and chunk_summaries:
                                try:
                                    async with ClientSession() as session:
                                        chunk_markdown = self._generate_markdown_bulletin(chunk_summaries)
                                        chunk_ai_title = await self._generate_ai_title(chunk_markdown, session)
                                        if chunk_ai_title:
                                            chunk_title = chunk_ai_title
                                            logger.info(f"Generated AI title for '{group_name}' chunk {chunk_key}: '{chunk_ai_title[:120]}'")
                                except Exception as e:
                                    logger.debug(f"AI title generation failed for chunk {chunk_key}: {e}")
                            if not chunk_title:
                                # Heuristic concatenation before intro-based fallback
                                try:
                                    chunk_titles = [ (s.get('item_title') or s.get('title','')).strip() for s in chunk_summaries if (s.get('item_title') or s.get('title')) ]
                                    if chunk_titles:
                                        heuristic = ", ".join(chunk_titles[:5])[:120]
                                        chunk_title = f"{group_name.title()}: {heuristic}".rstrip(' ,')
                                except Exception:
                                    pass
                                if not chunk_title:
                                    try:
                                        chunk_title = self._generate_title_from_introduction(chunk_intro or "", group_name, chunk_key)
                                    except Exception:
                                        chunk_title = f"{group_name.title()} Bulletin #{chunk_key.split('-')[-1]}"
                            await self.db.execute('create_bulletin',
                                                   group_name=group_name,
                                                   session_key=chunk_key,
                                                   introduction=chunk_intro or "",
                                                   summary_ids=chunk_ids,
                                                   feed_slugs=feed_slugs,
                                                   title=chunk_title)
                            logger.info(f"Created bulletin record for '{group_name}' session '{chunk_key}' with {len(chunk_ids)} summaries (title='{chunk_title[:80] if chunk_title else ''}')")
                
                except Exception as e:
                    logger.warning(f"Failed to create bulletin record for '{group_name}': {e}")
            else:
                logger.warning(f"No bulletin created for '{group_name}' - no summaries were marked as published")
            
            logger.info(f"Successfully published {len(summaries)} summaries to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error publishing HTML bulletin for group '{group_name}': {e}")
            return False

    def _generate_html_content(self, summary_group: str, feed_slugs: List[str], summaries: List[Dict[str, Any]], introduction: Optional[str] = None, title_text: Optional[str] = None) -> str:
        """Generate HTML content for a summary group bulletin using Jinja2 templates."""
        template = env.get_template('bulletin.html')
        current_time = datetime.now(timezone.utc)
        sorted_topics = sorted(summaries, key=lambda x: x.get('topic', ''))
        topic_count = len({s.get('topic', 'General') for s in summaries}) if summaries else 0

        return template.render(
            summary_group=summary_group,
            feed_slugs=feed_slugs,
            summaries=summaries,
            introduction=introduction,
            title_text=title_text,
            current_time=current_time,
            sorted_topics=sorted_topics,
            topic_count=topic_count
        )

    @trace_span(
        "publish_all_rss_feeds",
        tracer_name="publisher",
        attr_from_args=lambda self, only_slugs=None: {"feed.only_slugs": ",".join(only_slugs) if only_slugs else ""},
    )
    async def publish_all_rss_feeds(self, only_slugs: Optional[List[str]] = None) -> int:
        """Publish RSS feeds for summary groups defined in feeds.yaml.

        If only_slugs is provided, publish only groups whose feed list intersects
        with the provided slugs (used for per-feed runs).
        """
        config_data = self._load_feeds_config()
        summaries_config = config_data.get('summaries', {})
        
        if not summaries_config:
            logger.warning("No summary groups found in feeds.yaml")
            return 0
        
        published_count = 0
        feeds_config = config_data.get('feeds', {}) if isinstance(config_data, dict) else {}
        for group_name, group_entry in summaries_config.items():
            # Support:
            # - "slug1, slug2"
            # - ["slug1", "slug2"]
            # - { feeds: "slug1, slug2"|[...], intro: bool }
            enable_intro = False
            if isinstance(group_entry, dict):
                feeds_value = group_entry.get('feeds', group_entry.get('list') or group_entry.get('sources'))
                if isinstance(feeds_value, str):
                    feed_slugs = [slug.strip() for slug in feeds_value.split(',') if slug.strip()]
                elif isinstance(feeds_value, list):
                    feed_slugs = feeds_value
                else:
                    feed_slugs = []
                enable_intro = bool(str(group_entry.get('intro', 'false')).strip().lower() == 'true' or group_entry.get('intro') is True)
            elif isinstance(group_entry, str):
                feed_slugs = [slug.strip() for slug in group_entry.split(',') if slug.strip()]
            else:
                feed_slugs = group_entry
            # Per-feed filtering
            if only_slugs:
                if not any(slug in feed_slugs for slug in only_slugs):
                    logger.debug(f"Skipping RSS for group '{group_name}' (no overlap with slugs: {only_slugs})")
                    continue
            # If group-level intro not explicitly enabled, allow per-feed opt-in to turn it on
            if not enable_intro and isinstance(feeds_config, dict):
                try:
                    for slug in feed_slugs or []:
                        fc = feeds_config.get(slug) or {}
                        iv = fc.get('intro')
                        if isinstance(iv, str):
                            ivb = iv.strip().lower() == 'true'
                        else:
                            ivb = bool(iv)
                        if ivb:
                            enable_intro = True
                            break
                except Exception:
                    pass
            
            if await self.publish_rss_feed(group_name, feed_slugs, enable_intro=enable_intro):
                published_count += 1
        
        # Generate RSS feeds index.html after publishing all feeds
        await self._write_index_html()
        
        return published_count

    @trace_span(
        "publish_all_html_bulletins",
        tracer_name="publisher",
        attr_from_args=lambda self, only_slugs=None: {"feed.only_slugs": ",".join(only_slugs) if only_slugs else ""},
    )
    async def publish_all_html_bulletins(self, only_slugs: Optional[List[str]] = None) -> int:
        """Publish HTML bulletins for summary groups defined in feeds.yaml.

        If only_slugs is provided, publish only groups whose feed list intersects
        with the provided slugs (used for per-feed runs).
        """
        config_data = self._load_feeds_config()
        summaries_config = config_data.get('summaries', {})
        
        if not summaries_config:
            logger.warning("No summary groups found in feeds.yaml")
            return 0
        
        published_count = 0
        feeds_config = config_data.get('feeds', {}) if isinstance(config_data, dict) else {}
        for group_name, group_entry in summaries_config.items():
            # Support dict entries and per-group intro flag
            enable_intro = False
            if isinstance(group_entry, dict):
                feeds_value = group_entry.get('feeds', group_entry.get('list') or group_entry.get('sources'))
                if isinstance(feeds_value, str):
                    feed_slugs = [slug.strip() for slug in feeds_value.split(',') if slug.strip()]
                elif isinstance(feeds_value, list):
                    feed_slugs = feeds_value
                else:
                    feed_slugs = []
                enable_intro = bool(str(group_entry.get('intro', 'false')).strip().lower() == 'true' or group_entry.get('intro') is True)
            elif isinstance(group_entry, str):
                feed_slugs = [slug.strip() for slug in group_entry.split(',') if slug.strip()]
            else:
                feed_slugs = group_entry
            # Per-feed filtering
            if only_slugs:
                if not any(slug in feed_slugs for slug in only_slugs):
                    logger.debug(f"Skipping HTML bulletin for group '{group_name}' (no overlap with slugs: {only_slugs})")
                    continue
            # If group-level intro not explicitly enabled, allow per-feed opt-in to turn it on
            if not enable_intro and isinstance(feeds_config, dict):
                try:
                    for slug in feed_slugs or []:
                        fc = feeds_config.get(slug) or {}
                        iv = fc.get('intro')
                        if isinstance(iv, str):
                            ivb = iv.strip().lower() == 'true'
                        else:
                            ivb = bool(iv)
                        if ivb:
                            enable_intro = True
                            break
                except Exception:
                    pass
            
            if await self.publish_html_bulletin(group_name, feed_slugs, enable_intro=enable_intro):
                published_count += 1
        
        return published_count

    @trace_span("publish_all_content", tracer_name="publisher")
    async def publish_all_content(self) -> Tuple[int, int]:
        """Publish both HTML bulletins and RSS feeds for all summary groups, then generate all index files."""
        logger.info("Publishing all content (HTML bulletins and RSS feeds)...")
        
        # First publish HTML bulletins (which marks summaries as published)
        html_count = await self.publish_all_html_bulletins()
        
        # Then publish RSS feeds (which uses the published summaries)
        rss_count = await self.publish_all_rss_feeds()
        
        # Then publish passthrough feeds (raw per-feed RSS)
        pt_count = await self.publish_passthrough_feeds()
        
        # Generate all index files
        await self._write_bulletins_index_html()
        await self._write_main_index_html()
        
        logger.info(f"Published {html_count} HTML bulletins, {rss_count} summary RSS feeds, {pt_count} passthrough RSS feeds")
        return html_count, rss_count

    async def upload_to_azure(self, force: bool = False, sync_delete: Optional[bool] = None) -> Optional[Dict[str, Tuple[int, int, int]]]:
        """Upload all published content to Azure storage.
        
        Args:
            force: Force upload all files even if unchanged
            sync_delete: If True, delete remote files not present locally. Defaults to config flag.
            
        Returns:
            Dictionary with upload results per directory (uploaded, skipped, deleted), or None if Azure upload is disabled
        """
        if not self.azure_uploader or not self.azure_uploader.enabled:
            logger.debug("Azure upload skipped - not configured or disabled")
            return None
        
        logger.info("🌥️ Uploading content to Azure storage...")
        try:
            if sync_delete is None:
                sync_delete = bool(config.AZURE_UPLOAD_SYNC_DELETE)
            if sync_delete:
                logger.info("Remote deletion enabled for Azure upload (sync delete)")
            else:
                logger.info("Remote deletion disabled for Azure upload (safe mode)")
            results = await self.azure_uploader.sync_public_directory(self.public_dir, force=force, sync=bool(sync_delete))
            
            # Use the built-in summary printer
            self.azure_uploader.print_sync_summary(results)
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Azure upload failed: {e}")
            return None

    @trace_span("publish_all_content_with_upload", tracer_name="publisher", attr_from_args=lambda self, force_upload=False: {"azure.upload.force": bool(force_upload)})
    async def publish_all_content_with_upload(self, force_upload: bool = True, sync_delete: Optional[bool] = None) -> Tuple[int, int, Optional[Dict[str, Tuple[int, int, int]]]]:
        """Publish all content and optionally upload to Azure storage.
        
        Args:
            force_upload: Force upload all files to Azure even if unchanged
            
        Returns:
            Tuple of (html_count, rss_count, azure_results)
        """
        # Publish content locally
        html_count, rss_count = await self.publish_all_content()

        # Upload to Azure if enabled
        azure_results = await self.upload_to_azure(force=force_upload, sync_delete=sync_delete)

        return html_count, rss_count, azure_results

    async def _check_existing_bulletins_for_summaries(self, group_name: str, summary_ids: List[int]) -> List[str]:
        """Check if any bulletins already exist for the given summary IDs.
        
        Args:
            group_name: The summary group name
            summary_ids: List of summary IDs to check
            
        Returns:
            List of session keys for existing bulletins that contain these summaries
        """
        if not summary_ids:
            return []
            
        try:
            # Query the database to see if any of these summaries are already in bulletins
            cursor = self.db.conn.cursor()
            
            # Create placeholders for the IN clause
            placeholders = ','.join('?' for _ in summary_ids)
            
            query = f"""
                SELECT DISTINCT b.session_key
                FROM bulletins b
                JOIN bulletin_summaries bs ON b.id = bs.bulletin_id
                WHERE b.group_name = ? AND bs.summary_id IN ({placeholders})
            """
            
            params = [group_name] + summary_ids
            cursor.execute(query, params)
            
            results = cursor.fetchall()
            session_keys = [row[0] for row in results]
            
            cursor.close()
            return session_keys
            
        except Exception as e:
            logger.warning(f"Error checking existing bulletins for group {group_name}: {e}")
            return []
