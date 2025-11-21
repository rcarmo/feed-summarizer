#!/usr/bin/env python3
"""
Mastodon List fetcher and formatter.

This module fetches statuses from a Mastodon List timeline and converts them
into HTML items suitable for storage in the existing database schema.

No external dependencies beyond aiohttp/pyyaml that are already present.
Prefers asyncio and functional helpers.
"""

from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from aiohttp import ClientSession, ClientError, ClientTimeout
from utils import clean_html_to_markdown

from config import get_logger, config

logger = get_logger("mastodon")


def _parse_iso8601(ts: str) -> int:
    """Convert Mastodon ISO8601 timestamp to Unix epoch seconds (UTC)."""
    if not ts:
        return int(datetime.now(timezone.utc).timestamp())
    try:
        # Ensure timezone awareness
        if ts.endswith("Z"):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(ts)
        return int(dt.timestamp())
    except Exception:
        return int(datetime.now(timezone.utc).timestamp())


def _format_counts(status: Dict[str, Any]) -> str:
    """Render reactions/replies/favourites counters as a small HTML footer."""
    replies = status.get("replies_count", 0)
    boosts = status.get("reblogs_count", 0)
    favs = status.get("favourites_count", 0)
    return (
        f"<p>💬{replies} 🔁{boosts} ⭐{favs}</p>"
    )


def _format_footer(display_name: str, username: str, url: str, created_at: str) -> str:
    try:
        dt = _parse_iso8601(created_at)
        local_str = datetime.fromtimestamp(dt, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        local_str = created_at
    safe_display = display_name or username or ""
    return f"<p><small>- {safe_display} (@{username}) <a href=\"{url}\">{local_str}</a></small></p>"


def _format_attachments(attachments: List[Dict[str, Any]]) -> str:
    if not attachments:
        return ""
    parts: List[str] = []
    for att in attachments:
        preview = att.get("preview_url") or att.get("url")
        link = att.get("url") or preview
        title = att.get("description") or ""
        if link and preview:
            parts.append(f"<a href=\"{link}\"><img src=\"{preview}\" alt=\"{title}\"/></a>")
        elif link:
            parts.append(f"<a href=\"{link}\">{title or 'attachment'}</a>")
    return "".join(parts)


def render_status_html(status: Dict[str, Any]) -> Dict[str, Any]:
    """Render a Mastodon status (including boosts/replies) into a DB-ready item.

    Returns a dict with keys: title, url, guid, body, date.
    """
    account = status.get("account", {})
    username = account.get("username") or account.get("acct") or "unknown"
    display_name = account.get("display_name") or username
    url = status.get("url") or status.get("uri") or ""
    guid = status.get("uri") or url or status.get("id") or ""
    created_at = status.get("created_at") or ""

    # Base title and body
    title = f"@{username}"
    body = status.get("content") or ""

    # Sensitive / CW
    if status.get("sensitive"):
        spoiler = status.get("spoiler_text") or "Content Warning"
        title = f"@{username}: Content Warning: {spoiler}"

    # Boosts (reblogs)
    reblog = status.get("reblog")
    if reblog:
        rb_acc = reblog.get("account", {})
        rb_user = rb_acc.get("username") or rb_acc.get("acct") or "unknown"
        title = f"@{username} boosted @{rb_user}"
        inner_footer = _format_footer(
            rb_acc.get("display_name") or rb_user,
            rb_user,
            reblog.get("url") or reblog.get("uri") or url,
            reblog.get("created_at") or created_at,
        ) + _format_counts(reblog)
        body = (
            f"<blockquote>{reblog.get('content','')}"
            f"{_format_attachments(reblog.get('media_attachments') or [])}"
            f"{inner_footer}</blockquote>"
        )

    # Replies
    elif status.get("in_reply_to_id") and status.get("mentions"):
        first = (status.get("mentions") or [{}])[0]
        reply_to = first.get("username") or first.get("acct") or ""
        if reply_to:
            title = f"@{username} replied to @{reply_to}"

    # Polls (minimal marker)
    if status.get("poll") and "[poll]" not in body:
        body += "\n<p><em>[poll]</em></p>"

    # Media attachments for original statuses
    if not reblog:
        body = (
            f"{body}"
            f"{_format_attachments(status.get('media_attachments') or [])}"
        )

    # Footer
    body += _format_footer(display_name, username, url, created_at)
    body += _format_counts(status)

    # Clean HTML and convert to Markdown for storage consistency
    body_md = clean_html_to_markdown(body)

    return {
        "title": title,
        "url": url,
        "guid": guid,
    "body": body_md,
        "date": _parse_iso8601(created_at),
    }


async def fetch_list_timeline(list_url: str, token: str, limit: int = 40, session: Optional[ClientSession] = None, proxy_url: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    """Fetch statuses from a Mastodon List timeline using a bearer token.

    Args:
        list_url: Full API URL for the list timeline, e.g.
                  https://mastodon.social/api/v1/timelines/list/46540
        token:    Bearer token with permission to read the list
        limit:    Max statuses to fetch (server may cap)
        session:  Optional shared aiohttp ClientSession to reuse
        proxy_url: Optional HTTP proxy to use for this request

    Returns:
        List of status dicts on success, or None on error
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": config.USER_AGENT,
        "Accept": "application/json",
    }
    params = {"limit": str(limit)}

    try:
        base_timeout = max(int(config.HTTP_TIMEOUT), 1)
        timeout_seconds = base_timeout * 6 if proxy_url else base_timeout
        timeout = ClientTimeout(total=timeout_seconds)
        request_kwargs = {
            "headers": headers,
            "params": params,
            "allow_redirects": True,
            "timeout": timeout,
        }
        if proxy_url:
            request_kwargs["proxy"] = proxy_url

        async def _execute(client: ClientSession) -> Optional[List[Dict[str, Any]]]:
            async with client.get(list_url, **request_kwargs) as resp:
                resp.raise_for_status()
                data = await resp.json()
                if isinstance(data, list):
                    return data
                logger.error(f"Unexpected Mastodon response format: {type(data)}")
                return None

        if session is None:
            async with ClientSession(timeout=timeout) as owned_session:
                return await _execute(owned_session)
        return await _execute(session)
    except ClientError as e:
        logger.error(f"Mastodon API error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error calling Mastodon API: {e}")
    return None


