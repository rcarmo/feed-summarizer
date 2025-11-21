#!/usr/bin/env python3
"""Common error types shared across modules.

Provides shared lightweight exceptions to avoid circular imports.
"""

from typing import Dict, Any, Optional


class ContentFilterError(Exception):
    """Raised when Azure OpenAI content filtering blocks a response.

    Attributes:
        details: Optional provider-specific payload for diagnostics.
    """

    def __init__(self, message: str = "Content filtered by Azure OpenAI", details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}

__all__ = ["ContentFilterError"]
