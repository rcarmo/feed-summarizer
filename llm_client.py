#!/usr/bin/env python3
"""Async Azure OpenAI helper providing `chat_completion` with retry, content filter handling,
normalized content extraction and optional post-processing. Returns `None` on exhausted
retries or non-filter failures."""
from __future__ import annotations
from typing import List, Dict, Any, Optional, Callable
from asyncio import sleep

from config import config, get_logger
from errors import ContentFilterError

try:  # Guarded import (dependency declared in requirements.txt)
    from openai import AsyncAzureOpenAI, OpenAIError
except ImportError:  # pragma: no cover
    AsyncAzureOpenAI = None  # type: ignore
    OpenAIError = Exception  # type: ignore

logger = get_logger("llm_client")

_client: Any = None


def _get_client() -> Optional[Any]:
    """Instantiate and cache the Azure OpenAI async client if configuration is present."""
    global _client
    if _client is not None:
        return _client
    if not AsyncAzureOpenAI:
        return None
    if not (config.OPENAI_API_KEY and config.AZURE_ENDPOINT and config.OPENAI_API_VERSION and config.DEPLOYMENT_NAME):
        logger.debug("Missing Azure OpenAI config; client will not initialize")
        return None
    endpoint = (
        f"https://{config.AZURE_ENDPOINT}" if not str(config.AZURE_ENDPOINT).startswith("http") else config.AZURE_ENDPOINT
    )
    _client = AsyncAzureOpenAI(
        api_key=config.OPENAI_API_KEY,
        api_version=config.OPENAI_API_VERSION,
        azure_endpoint=endpoint,
    )
    return _client


async def chat_completion(
    messages: List[Dict[str, str]] = None,
    *,
    purpose: str = "generic",
    retries: Optional[int] = None,
    postprocess: Optional[Callable[[str], str]] = None,
    client_override: Optional[Any] = None,
) -> Optional[str]:
    """Execute an Azure OpenAI chat completion. Raises `ContentFilterError` on policy violations."""
    if messages is None:
        logger.error("chat_completion called without messages list")
        return None

    client = client_override or _get_client()
    if client is None:
        logger.warning("Azure OpenAI client unavailable; skipping %s", purpose)
        return None

    remaining = retries if retries is not None else config.SUMMARIZER_MAX_RETRIES
    attempt = 0
    model_name = config.DEPLOYMENT_NAME

    while attempt <= remaining:
        params: Dict[str, Any] = {
            "model": model_name,
            "messages": messages,
        }
        try:
            resp = await client.chat.completions.create(**params)
            choices = getattr(resp, "choices", None) or []
            if not choices:
                logger.error("No choices in %s response: %s", purpose, resp)
                return None
            def _extract_text(choice: Any) -> str:
                message = getattr(choice, "message", {}) or {}
                if isinstance(message, dict) and message.get("refusal"):
                    return ""
                content = getattr(message, "content", None) if not isinstance(message, dict) else message.get("content")
                if isinstance(content, str):
                    return content.strip()
                if isinstance(content, list):
                    texts: List[str] = []
                    for part in content:
                        if isinstance(part, dict):
                            ptype = part.get("type")
                            txt = part.get("text")
                            if isinstance(txt, str) and txt.strip():
                                texts.append(txt.strip())
                            else:
                                if ptype not in ("text", "output_text", None):
                                    logger.debug("Ignoring non-text part type=%s keys=%s", ptype, list(part.keys()))
                    return "\n".join(texts).strip()
                return ""
            fragments: List[str] = []
            refusal_detected = False
            for ch in choices:
                msg_obj = getattr(ch, "message", {}) or {}
                refusal_flag = msg_obj.get("refusal") if isinstance(msg_obj, dict) else getattr(msg_obj, "refusal", None)
                if refusal_flag:
                    refusal_detected = True
                    logger.warning("Refusal detected in %s response: %s", purpose, refusal_flag)
                txt = _extract_text(ch)
                if txt:
                    fragments.append(txt)
            if refusal_detected and not fragments:
                logger.warning("All choices refused for %s; returning None", purpose)
                return None
            raw = "\n".join(fragments).strip()
            if not raw:
                finish_reasons = {getattr(c, "finish_reason", None) for c in choices if getattr(c, "finish_reason", None)}
                if "length" in finish_reasons:
                    try:
                        logger.warning(
                            "Truncated output with empty content (%s); placeholder will be returned. choices=%s (no explicit token limit)",
                            purpose,
                            [
                                {
                                    "finish_reason": getattr(c, "finish_reason", None),
                                    "has_message": bool(getattr(c, "message", None)),
                                    "message_content_type": type(getattr(getattr(c, "message", None), "content", None)).__name__,
                                    "message_content_repr": repr(getattr(getattr(c, "message", None), "content", None))[:300],
                                }
                                for c in choices
                            ],
                        )
                    except Exception:
                        logger.warning("Truncated output with empty content (%s); returning placeholder", purpose)
                    raw = "[Truncated output: no content returned]"
                else:
                    logger.error("Empty content in %s response despite choices (finish_reasons=%s)", purpose, finish_reasons)
                    return None
            return postprocess(raw) if postprocess else raw
        except OpenAIError as e:
            attempt += 1
            err_body = getattr(e, "body", {}) or {}
            error_obj = err_body.get("error") if isinstance(err_body, dict) else None
            code = (error_obj or {}).get("code") if isinstance(error_obj, dict) else None
            # Treat token parameter errors as generic failures (no token params are sent).
            inner_code = (error_obj or {}).get("innererror", {}).get("code") if isinstance(error_obj, dict) else None
            if code == "content_filter" or inner_code == "ResponsibleAIPolicyViolation":
                raise ContentFilterError(message=(error_obj or {}).get("message", "Content filtered"), details=error_obj or {})
            if attempt > remaining:
                logger.error("%s request failed after %d retries: %s", purpose, remaining, e)
                return None
            delay = config.SUMMARIZER_RETRY_DELAY_BASE * (2 ** (attempt - 1))
            logger.warning("%s transient OpenAI error: %s. Backoff %ss (attempt %d/%d)", purpose, e, delay, attempt, remaining)
            await sleep(delay)
        except Exception as e:
            attempt += 1
            body = getattr(e, "body", {}) or {}
            error_obj = body.get("error") if isinstance(body, dict) else None
            if isinstance(error_obj, dict):
                code = error_obj.get("code")
                inner_code = (error_obj.get("innererror") or {}).get("code") if isinstance(error_obj.get("innererror"), dict) else None
                if code == "content_filter" or inner_code == "ResponsibleAIPolicyViolation":
                    raise ContentFilterError(message=error_obj.get("message", "Content filtered"), details=error_obj)
            if attempt > remaining:
                logger.error("%s unexpected failure after %d retries: %s", purpose, remaining, e)
                return None
            delay = config.SUMMARIZER_RETRY_DELAY_BASE * (2 ** (attempt - 1))
            logger.warning("%s unexpected error: %s. Backoff %ss (attempt %d/%d)", purpose, e, delay, attempt, remaining)
            await sleep(delay)

    return None


__all__ = ["chat_completion"]
