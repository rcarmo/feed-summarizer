#!/usr/bin/env python3
"""
Utility classes and functions for the feed processing system.

This module contains shared utilities used by both the fetcher and summarizer,
including rate limiting, common validation functions, and other helper utilities.
"""

from asyncio import Lock, sleep
from collections import Counter
from hashlib import blake2b
from time import time
from typing import Optional
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from markdownify import markdownify as md

# Import config to use unified logging
from config import config, get_logger

# Module-specific logger
logger = get_logger("utils")


class RateLimiter:
    """A token bucket rate limiter for controlling request rates.
    
    This class implements a simple rate limiter that ensures requests
    don't exceed a specified rate limit by introducing delays when necessary.
    """
    
    def __init__(self, requests_per_minute: int):
        """Initialize the rate limiter.
        
        Args:
            requests_per_minute: Maximum number of requests allowed per minute.
                                If 0 or negative, no rate limiting is applied.
        """
        self.requests_per_minute = requests_per_minute
        self.min_interval = 60.0 / requests_per_minute if requests_per_minute > 0 else 0
        self.last_request_time = 0
        self._lock = Lock()
    
    async def acquire(self):
        """Acquire permission to make a request, waiting if necessary to respect rate limits.
        
        This method will block (sleep) if necessary to maintain the configured rate limit.
        If rate limiting is disabled (requests_per_minute <= 0), this method returns immediately.
        """
        if self.min_interval <= 0:
            return  # No rate limiting
            
        async with self._lock:
            current_time = time()
            time_since_last = current_time - self.last_request_time
            
            if time_since_last < self.min_interval:
                wait_time = self.min_interval - time_since_last
                logger.debug(f"Rate limiting: waiting {wait_time:.2f} seconds")
                await sleep(wait_time)
                
            self.last_request_time = time()  # Update to current time after potential wait


def validate_url(url: str) -> bool:
    """Validate if a string is a properly formatted URL.
    
    Args:
        url: The URL string to validate
        
    Returns:
        True if the URL appears to be valid, False otherwise
    """
    if not url or not isinstance(url, str):
        return False
    
    url = url.strip()
    if not url:
        return False
        
    # Basic URL validation
    return url.startswith(('http://', 'https://')) and '.' in url


def safe_filename(filename: str, max_length: int = 255) -> str:
    """Convert a string to a safe filename by removing/replacing problematic characters.
    
    Args:
        filename: The original filename string
        max_length: Maximum allowed length for the filename
        
    Returns:
        A sanitized filename safe for filesystem use
    """
    if not filename:
        return "untitled"
    
    # Remove or replace problematic characters
    safe_name = re.sub(r'[<>:"/\\|?*]', '_', filename)
    safe_name = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', safe_name)  # Remove control characters
    safe_name = safe_name.strip('. ')  # Remove leading/trailing dots and spaces
    
    if not safe_name:
        return "untitled"
        
    # Truncate if too long
    if len(safe_name) > max_length:
        safe_name = safe_name[:max_length].rstrip('. ')
        
    return safe_name


def format_duration(seconds: float) -> str:
    """Format a duration in seconds to a human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string (e.g., "1h 23m 45s")
    """
    if seconds < 0:
        return "0s"
        
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:  # Always show seconds if nothing else
        parts.append(f"{secs}s")
        
    return " ".join(parts)


def truncate_string(text: str, max_length: int, suffix: str = "...") -> str:
    """Truncate a string to a maximum length, adding a suffix if truncated.
    
    Args:
        text: The text to potentially truncate
        max_length: Maximum allowed length (including suffix)
        suffix: Suffix to add when truncating
        
    Returns:
        The original text or truncated version with suffix
    """
    if not text or len(text) <= max_length:
        return text
        
    if len(suffix) >= max_length:
        return text[:max_length]
        
    return text[:max_length - len(suffix)] + suffix


class RetryHelper:
    """Helper class for implementing retry logic with exponential backoff."""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        """Initialize the retry helper.
        
        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay in seconds for exponential backoff
            max_delay: Maximum delay in seconds between retries
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate the delay for a given retry attempt.
        
        Args:
            attempt: The current attempt number (0-based)
            
        Returns:
            Delay in seconds (with exponential backoff)
        """
        delay = self.base_delay * (2 ** attempt)
        return min(delay, self.max_delay)
    
    async def sleep_for_attempt(self, attempt: int):
        """Sleep for the calculated delay for the given attempt.
        
        Args:
            attempt: The current attempt number (0-based)
        """
        delay = self.calculate_delay(attempt)
        if delay > 0:
            logger.debug(f"Retry delay: sleeping for {delay:.2f} seconds")
            await sleep(delay)


def clean_html_to_markdown(html_content: str, base_url: Optional[str] = None) -> str:
    """Sanitize HTML content and convert it to Markdown.

    Args:
        html_content: Raw HTML to sanitize
        base_url: Optional base URL used to resolve relative href/src values

    Behavior:
    - Removes dangerous elements (script/style/iframe/etc.)
    - Strips inline event handlers and javascript: URLs
    - Removes common tracking pixels
    - Resolves relative href/src to absolute URLs when ``base_url`` is provided; otherwise
      non-absolute references are neutralized (links -> ``#``, images removed)
    - Converts resulting HTML to Markdown with markdownify
    """
    if not html_content:
        return ""

    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # Remove potentially dangerous elements
        for tag in soup([
            "script", "style", "iframe", "form", "object", "embed", "noscript",
            "frame", "frameset", "applet", "meta", "base", "link"
        ]):
            tag.decompose()

        # Remove on* attributes and javascript: URLs
        for tag in soup.find_all(True):
            for attr in list(tag.attrs):
                if attr.lower().startswith('on'):
                    del tag[attr]
                if attr.lower() in ['href', 'src'] and tag.has_attr(attr):
                    val = str(tag[attr])
                    if val.lower().startswith('javascript:'):
                        del tag[attr]

        # Remove tracking pixels / tiny images
        for img in soup.find_all('img'):
            src = img.get('src', '')
            if re.search(r'(pixel|tracker|counter|spacer|blank|trans)', src, re.I) or \
               (re.search(r'\.(gif|png)$', src, re.I) and (img.get('height') in ('0', '1'))):
                img.decompose()

        def _rewrite_url(value: str, attr: str) -> Optional[str]:
            if not value:
                return None
            if attr == 'href' and value.startswith('mailto:'):
                return value
            if value.startswith(('http://', 'https://')):
                return value
            if base_url:
                try:
                    resolved = urljoin(base_url, value)
                except Exception:
                    return None
                if resolved and resolved.startswith(('http://', 'https://')):
                    return resolved
            return None

        # Force non-absolute URLs to safe defaults, resolving when possible
        for tag in soup.find_all(['a', 'img']):
            for attr in ['href', 'src']:
                if not tag.has_attr(attr):
                    continue
                val = str(tag[attr])
                if not val:
                    continue
                rewritten = _rewrite_url(val, attr)
                if rewritten:
                    tag[attr] = rewritten
                else:
                    if attr == 'href':
                        tag[attr] = '#'
                    else:
                        del tag[attr]

        # Convert to Markdown
        # Important: disable line wrapping to avoid inserting newlines inside URLs
        # which later break when converting back to HTML for RSS (href/src get split).
        # markdownify supports wrap_width; setting it to 0 disables wrapping.
        return md(str(soup), heading_style="ATX", wrap_width=0)
    except Exception as e:
        logger.error(f"Error cleaning HTML to Markdown: {e}")
        return html_content


INT64_MASK = (1 << 64) - 1


def encode_int64(value: Optional[int]) -> Optional[int]:
    """Encode an unsigned 64-bit value into SQLite-compatible signed range."""
    if value is None:
        return None
    masked = value & INT64_MASK
    if masked >= (1 << 63):
        masked -= 1 << 64
    return masked


def decode_int64(value: Optional[int]) -> Optional[int]:
    """Decode a signed SQLite integer back into the original 64-bit value."""
    if value is None:
        return None
    return value & INT64_MASK


def compute_simhash(text: Optional[str], hash_bits: int = 64) -> Optional[int]:
    """Compute a lightweight SimHash fingerprint for the provided text."""
    if not text:
        return None
    tokens = re.findall(r"\w+", text.lower())
    if not tokens:
        return None
    freq = Counter(tokens)
    bits = max(8, hash_bits)
    if bits % 8 != 0:
        bits -= bits % 8
    digest_size = bits // 8
    if digest_size <= 0:
        return None
    vector = [0] * bits
    for token, weight in freq.items():
        try:
            digest = blake2b(token.encode('utf-8'), digest_size=digest_size).digest()
        except Exception:
            continue
        value = int.from_bytes(digest, 'big')
        for bit in range(bits):
            if value & (1 << bit):
                vector[bit] += weight
            else:
                vector[bit] -= weight
    fingerprint = 0
    for bit, score in enumerate(vector):
        if score > 0:
            fingerprint |= (1 << bit)
    return fingerprint if fingerprint != 0 else None


def hamming_distance(value_a: Optional[int], value_b: Optional[int], bits: int = 64) -> Optional[int]:
    """Compute the Hamming distance between two integer fingerprints."""
    if value_a is None or value_b is None:
        return None
    mask_bits = max(1, bits)
    mask = (1 << mask_bits) - 1
    diff = (value_a ^ value_b) & mask
    try:
        return diff.bit_count()
    except AttributeError:
        # Python <3.8 fallback (unlikely, but keeps helper defensive)
        count = 0
        while diff:
            diff &= diff - 1
            count += 1
        return count
