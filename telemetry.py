#!/usr/bin/env python3
"""
Telemetry setup using OpenTelemetry and Azure Application Insights.

This module configures tracing for aiohttp client requests and key app spans,
exporting to Azure Monitor when an Application Insights connection string is
provided via environment variable.

Environment variables:
  - APPLICATIONINSIGHTS_CONNECTION_STRING or AZURE_MONITOR_CONNECTION_STRING
  - OTEL_SERVICE_NAME (default: feed-summarizer)
  - OTEL_ENVIRONMENT (maps to deployment.environment)
  - DISABLE_TELEMETRY=true to fully disable

The module is safe to import multiple times; initialization is idempotent.
"""

from __future__ import annotations

import os
import atexit
import logging
import threading
from typing import Optional
import asyncio

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.instrumentation.aiohttp_client import AioHttpClientInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.instrumentation.sqlite3 import SQLite3Instrumentor

try:
    # Azure Monitor exporter is optional; only used when connection string is present
    from azure.monitor.opentelemetry.exporter import (
        AzureMonitorTraceExporter,
        AzureMonitorLogExporter,
    )  # type: ignore
    _AZURE_AVAILABLE = True
    _AZURE_IMPORT_ERROR: Optional[str] = None
except Exception as _imp_err:
    AzureMonitorTraceExporter = None  # type: ignore
    AzureMonitorLogExporter = None  # type: ignore
    _AZURE_AVAILABLE = False
    _AZURE_IMPORT_ERROR = repr(_imp_err)

_init_lock = threading.Lock()
_initialized = False
_provider: Optional[TracerProvider] = None

_logger = logging.getLogger(__name__)


def init_telemetry(service_name: Optional[str] = None) -> None:
    """Initialize OpenTelemetry tracing and instrumentation.

    Safe to call multiple times. If DISABLE_TELEMETRY=true, it's a no-op.
    """
    global _initialized
    if os.environ.get("DISABLE_TELEMETRY", "false").lower() == "true":
        return
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return

        # Build resource describing this service
        svc = service_name or os.environ.get("OTEL_SERVICE_NAME", "feed-summarizer")
        env = os.environ.get("OTEL_ENVIRONMENT")
        attrs = {"service.name": svc}
        if env:
            attrs["deployment.environment"] = env
        resource = Resource.create(attrs)

        # If a provider was already set by external auto-instrumentation, reuse it
        existing = trace.get_tracer_provider()
        provider: TracerProvider
        if isinstance(existing, TracerProvider):
            provider = existing
            # Best effort: remove any ConsoleSpanExporter processors to avoid stdout noise
            try:
                asp = getattr(provider, "_active_span_processor", None)
                procs = []
                # MultiSpanProcessor has _span_processors/.span_processors
                if hasattr(asp, "_span_processors"):
                    procs = list(getattr(asp, "_span_processors", []) or [])
                elif hasattr(asp, "span_processors"):
                    procs = list(getattr(asp, "span_processors", []) or [])
                removed = 0
                kept = []
                for p in procs:
                    exp = getattr(p, "span_exporter", None) or getattr(p, "exporter", None)
                    if isinstance(exp, ConsoleSpanExporter):
                        try:
                            p.shutdown()
                        except Exception:
                            pass
                        removed += 1
                    else:
                        kept.append(p)
                if removed and hasattr(asp, "_span_processors"):
                    setattr(asp, "_span_processors", kept)
                    _logger.info("Telemetry: removed %d ConsoleSpanExporter processor(s) from existing provider", removed)
            except Exception:
                # Non-fatal; continue
                pass
        else:
            # Configure a fresh tracer provider
            provider = TracerProvider(resource=resource)

        # Exporters: prefer Azure if connection string present
        # Resolve connection string with fallbacks (older env var styles)
        conn_source = "none"
        conn = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING") or os.environ.get(
            "AZURE_MONITOR_CONNECTION_STRING"
        )
        if conn:
            conn_source = "connection_string"
        if not conn:
            # Accept legacy instrumentation key env vars and build a connection string
            ikey = (
                os.environ.get("APPLICATIONINSIGHTS_INSTRUMENTATIONKEY")
                or os.environ.get("APPLICATIONINSIGHTS_INSTRUMENTATION_KEY")
                or os.environ.get("APPINSIGHTS_INSTRUMENTATIONKEY")
            )
            if ikey:
                conn = f"InstrumentationKey={ikey}"
                conn_source = "instrumentation_key"
        if conn and _AZURE_AVAILABLE:
            try:
                az_exporter = AzureMonitorTraceExporter.from_connection_string(conn)  # type: ignore
                provider.add_span_processor(BatchSpanProcessor(az_exporter))
                _logger.info(
                    "Telemetry initialized: Azure Monitor trace exporter enabled (service=%s, source=%s)",
                    svc,
                    conn_source,
                )
            except Exception:
                # Do not fallback to console exporter; keep spans internal only
                _logger.warning("Telemetry init: failed to enable Azure exporter; spans will not be exported")
        else:
            # No Azure configured: do not export to console to avoid stdout noise
            _logger.info(
                "Telemetry initialized without Azure exporter (service=%s, source=%s); no spans will be exported",
                svc,
                conn_source,
            )
            if conn and not _AZURE_AVAILABLE and _AZURE_IMPORT_ERROR:
                _logger.warning(
                    "Azure exporter package unavailable; install 'azure-monitor-opentelemetry-exporter'. Import error: %s",
                    _AZURE_IMPORT_ERROR,
                )

        # Only set provider if it wasn't already set
        if not isinstance(existing, TracerProvider):
            trace.set_tracer_provider(provider)
        global _provider
        _provider = provider

        # Instrument libraries
        try:
            AioHttpClientInstrumentor().instrument()
        except Exception:
            pass
        try:
            # Inject trace/span ids into log records as otelTraceID / otelSpanID without changing format
            LoggingInstrumentor().instrument()
        except Exception:
            pass
        try:
            SQLite3Instrumentor().instrument()
        except Exception:
            pass

        _initialized = True

        # Ensure spans flush on interpreter exit for short-lived commands
        try:
            def _shutdown():
                try:
                    # TracerProvider.shutdown() flushes BatchSpanProcessor
                    if _provider:
                        _provider.shutdown()
                except Exception:
                    pass

            atexit.register(_shutdown)
        except Exception:
            pass

        # One-time startup self-test span indicating exporter status
        try:
            tracer = get_tracer("telemetry")
            with tracer.start_as_current_span("telemetry.startup") as span:
                span.set_attribute("exporter.azure.enabled", bool(conn and _AZURE_AVAILABLE))
                span.set_attribute("exporter.connection.source", conn_source)
                span.set_attribute("service.name", svc)
                if env:
                    span.set_attribute("deployment.environment", env)
                # Also attach a summary of active span processors/exporters for diagnostics
                try:
                    asp = getattr(provider, "_active_span_processor", None)
                    procs = []
                    if hasattr(asp, "_span_processors"):
                        procs = list(getattr(asp, "_span_processors", []) or [])
                    elif hasattr(asp, "span_processors"):
                        procs = list(getattr(asp, "span_processors", []) or [])
                    names = []
                    for p in procs:
                        exp = getattr(p, "span_exporter", None) or getattr(p, "exporter", None)
                        names.append(type(exp).__name__ if exp else type(p).__name__)
                    span.set_attribute("exporter.processors", ",".join(names))
                    _logger.info("Telemetry processors active: %s", ",".join(names) or "<none>")
                except Exception:
                    pass
        except Exception:
            pass

        # Attach OpenTelemetry log handler if Azure logs exporter is available and conn is set
        try:
            if conn and _AZURE_AVAILABLE:
                from opentelemetry.sdk._logs import (
                    LoggerProvider,
                    BatchLogRecordProcessor,
                    LoggingHandler,
                )
                from opentelemetry._logs import set_logger_provider

                lp = LoggerProvider(resource=resource)
                exporter = AzureMonitorLogExporter.from_connection_string(conn)  # type: ignore
                lp.add_log_record_processor(BatchLogRecordProcessor(exporter))
                set_logger_provider(lp)

                handler = LoggingHandler(level=None, logger_provider=lp)
                root_logger = logging.getLogger()
                if not any(isinstance(h, type(handler)) for h in root_logger.handlers):
                    root_logger.addHandler(handler)
        except Exception:
            # Non-fatal
            pass


def get_tracer(name: str = "feed-summarizer"):
    """Get the OpenTelemetry tracer for a named subsystem."""
    return trace.get_tracer(name)


def trace_span(
    span_name: str | None = None,
    *,
    tracer_name: str | None = None,
    static_attrs: dict | None = None,
    attr_from_args: Optional[callable] = None,
):
    """Decorator to wrap a function call in an OpenTelemetry span.

    Args:
        span_name: Name of the span (defaults to module.funcname)
        tracer_name: Tracer name (defaults to span_name or 'feed-summarizer')
        static_attrs: Dict of attributes to set on the span
        attr_from_args: Callable taking (*args, **kwargs) and returning a dict
                        of attributes to set on the span

    Works with sync and async functions.
    """

    def _decorator(func):
        name = span_name or f"{func.__module__}.{func.__name__}"
        tname = tracer_name or name.split(".")[0] or "feed-summarizer"
        tracer = get_tracer(tname)

        def _set_attrs(span, args, kwargs):
            if not span:
                return
            try:
                if static_attrs:
                    for k, v in static_attrs.items():
                        span.set_attribute(k, v)
                if callable(attr_from_args):
                    dyn = attr_from_args(*args, **kwargs) or {}
                    for k, v in dyn.items():
                        span.set_attribute(k, v)
            except Exception:
                # Never break the app on attribute setting
                pass

        if hasattr(func, "__aiter__") or hasattr(func, "__anext__"):
            # Uncommon case: async iterator; skip for now
            return func

        if asyncio.iscoroutinefunction(func):

            async def _aw(*args, **kwargs):
                with tracer.start_as_current_span(name) as span:
                    _set_attrs(span, args, kwargs)
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        try:
                            if span:
                                span.record_exception(e)
                                span.set_status(Status(StatusCode.ERROR))
                        finally:
                            pass
                        raise

            _aw.__name__ = func.__name__
            _aw.__doc__ = func.__doc__
            _aw.__qualname__ = getattr(func, "__qualname__", func.__name__)
            return _aw
        else:

            def _w(*args, **kwargs):
                with tracer.start_as_current_span(name) as span:
                    _set_attrs(span, args, kwargs)
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        try:
                            if span:
                                span.record_exception(e)
                                span.set_status(Status(StatusCode.ERROR))
                        finally:
                            pass
                        raise

            _w.__name__ = func.__name__
            _w.__doc__ = func.__doc__
            _w.__qualname__ = getattr(func, "__qualname__", func.__name__)
            return _w

    return _decorator


def enable_log_export() -> Optional[object]:
    """Enable OpenTelemetry log export to Azure Monitor if configured.

    Returns the installed logging handler (if any) so callers may manage it.
    """
    if os.environ.get("DISABLE_TELEMETRY", "false").lower() == "true":
        return None
    if not _AZURE_AVAILABLE:
        return None
    conn = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING") or os.environ.get(
        "AZURE_MONITOR_CONNECTION_STRING"
    )
    if not conn:
        return None
    try:
        # Import inside to avoid hard dependency if logs are not desired
        from opentelemetry.sdk._logs import (
            LoggerProvider,
            BatchLogRecordProcessor,
            LoggingHandler,
        )
        from opentelemetry._logs import set_logger_provider

        resource = trace.get_tracer_provider().resource if trace.get_tracer_provider() else Resource.create({})
        lp = LoggerProvider(resource=resource)
        exporter = AzureMonitorLogExporter.from_connection_string(conn)  # type: ignore
        lp.add_log_record_processor(BatchLogRecordProcessor(exporter))
        set_logger_provider(lp)

        # Create a handler that sends stdlib logging records to OTel logs
        handler = LoggingHandler(level=None, logger_provider=lp)
        return handler
    except Exception:
        return None
