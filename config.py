#!/usr/bin/env python3
"""
Configuration management for Feed Fetcher.

This module centralizes all configuration loading, validation, and management.
It handles environment variables, validation, and provides a clean interface
for accessing configuration values throughout the application.
"""

from os import environ, path, access, R_OK
from typing import Dict, Any
from logging import getLogger, basicConfig, StreamHandler, INFO, DEBUG, WARNING, ERROR
import sys
import yaml
from dotenv import load_dotenv

def _setup_global_logger():
    """Setup a single global logger for the entire application.
    
    This function configures the logging system for the entire feed processor application.
    It sets up a unified logging configuration that can be controlled via environment variables:
    
    Environment Variables:
        LOG_LEVEL: Set log level (DEBUG, INFO, WARNING, ERROR) - defaults to INFO
        LOG_TIMESTAMPS: Enable/disable timestamps in logs (true/false) - defaults to true
    
    The logger outputs to stdout with line buffering for real-time logging.
    All modules should use get_logger() to create module-specific loggers that inherit this configuration.
    """
    # Ensure unbuffered I/O for Python and any child processes
    try:
        environ["PYTHONUNBUFFERED"] = "1"
    except Exception:
        pass

    # Determine log level
    level_str = environ.get("LOG_LEVEL", "INFO").upper()
    level_map = {
        "DEBUG": DEBUG,
        "INFO": INFO, 
        "WARNING": WARNING,
        "ERROR": ERROR
    }
    level = level_map.get(level_str, INFO)
    
    # Check if timestamps should be disabled
    show_timestamps = environ.get("LOG_TIMESTAMPS", "true").lower() != "false"
    
    # Format: with or without timestamps
    if show_timestamps:
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    else:
        log_format = '%(name)s - %(levelname)s - %(message)s'
    
    # Setup basic configuration with unbuffered stdout/stderr
    basicConfig(
        level=level,
        format=log_format,
        handlers=[StreamHandler(sys.stdout)],
        force=True  # Force reconfiguration if already configured
    )
    
    # Ensure unbuffered output
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    # Reduce Azure SDK verbosity unless explicitly overridden
    azure_level_str = environ.get("AZURE_LOG_LEVEL", "WARNING").upper()
    environ["AZURE_LOG_LEVEL"] = azure_level_str
    azure_level = level_map.get(azure_level_str, WARNING)
    try:
        for name in (
            "azure",
            "azure.core",
            "azure.core.pipeline.policies.http_logging_policy",
            "azure.monitor",
            "azure.monitor.opentelemetry",
            "azure.monitor.opentelemetry.exporter",
        ):
            az_logger = getLogger(name)
            az_logger.setLevel(azure_level)
    except Exception:
        # Never fail app startup due to logging tweaks
        pass
    
    return getLogger("FeedProcessor")

def get_logger(name: str):
    """Get a module-specific logger with the unified configuration.
    
    This function creates a logger with a name in the format "FeedProcessor.{name}".
    All loggers created this way inherit the global logging configuration set by _setup_global_logger().
    
    Args:
        name: The logger name (e.g., "fetcher", "summarizer", "publisher")
        
    Returns:
        A logger instance with the unified configuration
        
    Example:
        logger = get_logger("mymodule")
        logger.info("This will appear as 'FeedProcessor.mymodule - INFO - This will appear...'")
    """
    return getLogger(f"FeedProcessor.{name}")

# Create single global logger instance
logger = _setup_global_logger()

class Config:
    """Configuration manager for Feed Fetcher.
    
    This class handles loading and validation of configuration from multiple sources:
    1. Environment variables
    2. .env file (if present)
    3. YAML secrets file (if SECRETS_FILE environment variable is set)
    4. feeds.yaml configuration file
    
    The loading order ensures that:
    - .env file variables override system environment variables
    - Secrets file variables override both system and .env variables
    - This allows for secure management of sensitive configuration
    
    Example secrets.yaml format:
    ```yaml
    environment:
      AZURE_ENDPOINT: "https://your-resource.openai.azure.com/"
      OPENAI_API_KEY: "your-api-key"
      AZURE_STORAGE_ACCOUNT: "yourstorageaccount"
      AZURE_STORAGE_KEY: "your-storage-key"
    ```
    """
    
    def __init__(self):
        """Initialize configuration with environment variables and validation."""
        self._load_environment()
        self._validate_and_set_config()
        self._load_feed_sources()
    
    def _load_environment(self):
        """Load environment variables from .env file and secrets file if present."""
        # Load .env file first
        dotenv_path = path.join(path.dirname(path.abspath(__file__)), '.env')
        if path.exists(dotenv_path):
            load_dotenv(dotenv_path)
            logger.info(f"Loaded environment variables from {dotenv_path}")
        
        # Load secrets file if SECRETS_FILE environment variable is set
        self._load_secrets_file()
    
    def _validate_positive_int(self, env_var: str, default: int, min_val: int = 1) -> int:
        """Validate and parse a positive integer environment variable."""
        try:
            value = int(environ.get(env_var, str(default)))
            if value < min_val:
                logger.warning(f"{env_var} must be at least {min_val}, using default {default}")
                return default
            return value
        except (ValueError, TypeError):
            logger.warning(f"Invalid {env_var} value, using default {default}")
            return default

    def _validate_positive_float(self, env_var: str, default: float, min_val: float = 0.1) -> float:
        """Validate and parse a positive float environment variable."""
        try:
            value = float(environ.get(env_var, str(default)))
            if value < min_val:
                logger.warning(f"{env_var} must be at least {min_val}, using default {default}")
                return default
            return value
        except (ValueError, TypeError):
            logger.warning(f"Invalid {env_var} value, using default {default}")
            return default
    
    def _validate_and_set_config(self):
        """Validate and set all configuration values."""
        # Basic configuration
        self.DATABASE_PATH = environ.get("DATABASE_PATH", "feeds.db")
        self.USER_AGENT = environ.get("USER_AGENT", "Mozilla/5.0 (compatible; RSSFetcher/1.0; +https://rss.carmo.io)")
        self.FORCE_REFRESH_FEEDS = environ.get("FORCE_REFRESH_FEEDS", "false").lower() == "true"

        # Timing configuration
        self.FETCH_INTERVAL_MINUTES = self._validate_positive_int("FETCH_INTERVAL_MINUTES", 30, 1)

        # HTTP request configuration
        self.MAX_RETRIES = self._validate_positive_int("MAX_RETRIES", 3, 1)
        self.RETRY_DELAY_BASE = self._validate_positive_float("RETRY_DELAY_BASE", 1.0, 0.1)
        self.HTTP_TIMEOUT = self._validate_positive_int("HTTP_TIMEOUT", 30, 5)
        self.MAX_REDIRECTS = self._validate_positive_int("MAX_REDIRECTS", 5, 0)

        # Rate limiting and cooldown configuration
        self.MIN_COOLDOWN_PERIOD = self._validate_positive_int("MIN_COOLDOWN_PERIOD", 3600, 60)
        self.READER_MODE_REQUESTS_PER_MINUTE = self._validate_positive_int("READER_MODE_REQUESTS_PER_MINUTE", 10, 1)

        # Data management configuration
        self.ENTRY_EXPIRATION_DAYS = self._validate_positive_int("ENTRY_EXPIRATION_DAYS", 365, 1)

        # Batch processing configuration
        self.SAVE_BATCH_SIZE = self._validate_positive_int("SAVE_BATCH_SIZE", 10, 1)
        self.READER_MODE_CONCURRENCY = self._validate_positive_int("READER_MODE_CONCURRENCY", 3, 1)

        # Retention & summarization windows
        # Max items physically retained per feed (oldest trimmed by count after new inserts)
        self.MAX_ITEMS_PER_FEED = self._validate_positive_int("MAX_ITEMS_PER_FEED", 400, 50)
        # Number of newest unsummarized items considered per feed when generating summaries
        self.SUMMARY_WINDOW_ITEMS = self._validate_positive_int("SUMMARY_WINDOW_ITEMS", 50, 10)

        # File size limits
        self.SCHEMA_FILE_SIZE_LIMIT_MB = self._validate_positive_int("SCHEMA_FILE_SIZE_LIMIT_MB", 10, 1)

        # OpenAI/Azure AI configuration for summarizer
        self.AZURE_ENDPOINT = environ.get("AZURE_ENDPOINT")
        # Normalize endpoint (strip scheme and trailing slashes) to avoid malformed URLs
        if self.AZURE_ENDPOINT:
            normalized = self.AZURE_ENDPOINT.strip()
            if normalized.lower().startswith("https://"):
                normalized = normalized[8:]
            elif normalized.lower().startswith("http://"):
                normalized = normalized[7:]
            normalized = normalized.strip("/")
            if normalized != self.AZURE_ENDPOINT:
                logger.info(f"Normalized AZURE_ENDPOINT to '{normalized}'")
            self.AZURE_ENDPOINT = normalized
        self.OPENAI_API_KEY = environ.get("OPENAI_API_KEY")
        self.DEPLOYMENT_NAME = environ.get("DEPLOYMENT_NAME")
        self.OPENAI_API_VERSION = environ.get("OPENAI_API_VERSION")

        # RSS publishing configuration
        self.RSS_BASE_URL = environ.get("RSS_BASE_URL", "https://example.com")

        # Azure storage configuration
        self.AZURE_STORAGE_ACCOUNT = environ.get("AZURE_STORAGE_ACCOUNT")
        self.AZURE_STORAGE_KEY = environ.get("AZURE_STORAGE_KEY")
        self.AZURE_STORAGE_CONTAINER = environ.get("AZURE_STORAGE_CONTAINER", "$web")
        # Whether to delete remote files not present locally during sync upload
        self.AZURE_UPLOAD_SYNC_DELETE = environ.get("AZURE_UPLOAD_SYNC_DELETE", "false").lower() == "true"

        # Summarizer-specific configuration (longer timeouts for AI API calls)
        self.SUMMARIZER_HTTP_TIMEOUT = self._validate_positive_int("SUMMARIZER_HTTP_TIMEOUT", 60, 10)
        self.SUMMARIZER_MAX_RETRIES = self._validate_positive_int("SUMMARIZER_MAX_RETRIES", 3, 1)
        self.SUMMARIZER_RETRY_DELAY_BASE = self._validate_positive_float("SUMMARIZER_RETRY_DELAY_BASE", 1.0, 0.1)
        self.SUMMARIZER_REQUESTS_PER_MINUTE = self._validate_positive_int("SUMMARIZER_REQUESTS_PER_MINUTE", 60, 1)
        # Token limiting removed (formerly SUMMARIZER_MAX_TOKENS); rely on model/service defaults.
        # Temperature removed from runtime configuration; model default is used.

        # Scheduler configuration
        self.SCHEDULER_TIMEZONE = environ.get("SCHEDULER_TIMEZONE", "UTC")
        self.SCHEDULER_RUN_IMMEDIATELY = environ.get("SCHEDULER_RUN_IMMEDIATELY", "false").lower() == "true"

        # File paths
        base_dir = path.dirname(path.abspath(__file__))
        # Workspace/data paths
        # DATA_PATH: base folder for generated artifacts (defaults to repo root)
        self.DATA_PATH = environ.get("DATA_PATH", base_dir)
        # PUBLIC_DIR: where HTML/RSS outputs are written (defaults to $DATA_PATH/public)
        self.PUBLIC_DIR = environ.get("PUBLIC_DIR", path.join(self.DATA_PATH, "public"))
        self.SCHEMA_FILE_PATH = path.join(base_dir, "schema.sql")
        self.FEEDS_CONFIG_PATH = path.join(base_dir, "feeds.yaml")
        self.PROMPT_CONFIG_PATH = path.join(base_dir, "prompt.yaml")
    
    def _load_secrets_file(self):
        """Load environment variable overrides from a YAML secrets file.
        
        If SECRETS_FILE environment variable is set, loads the specified YAML file
        and sets environment variables from it. This allows for secure management
        of sensitive configuration data.
        
    Expected YAML formats (both supported):
        ```yaml
    # Preferred: top-level mapping
    AZURE_ENDPOINT: "https://your-resource.openai.azure.com/"
    OPENAI_API_KEY: "your-api-key"
    AZURE_STORAGE_ACCOUNT: "yourstorageaccount"
    AZURE_STORAGE_KEY: "your-storage-key"
        
    # Backward-compatible: nested under `environment`
    # environment:
    #   AZURE_ENDPOINT: "https://your-resource.openai.azure.com/"
    #   OPENAI_API_KEY: "your-api-key"
    #   AZURE_STORAGE_ACCOUNT: "yourstorageaccount"
    #   AZURE_STORAGE_KEY: "your-storage-key"
        ```
        """
        secrets_file_path = environ.get("SECRETS_FILE")
        if not secrets_file_path:
            logger.info("SECRETS_FILE not set; relying on environment/.env for secrets")
            return
        
        try:
            # Check if file exists and is accessible
            if not path.isfile(secrets_file_path):
                logger.warning(f"Secrets file not found at {secrets_file_path}")
                return
            
            # Check if we have read permissions
            if not access(secrets_file_path, R_OK):
                logger.error(f"No read permission for secrets file at {secrets_file_path}")
                return
            
            # Check file size to prevent reading extremely large files
            file_size = path.getsize(secrets_file_path)
            max_size = 2 * 1024 * 1024  # 2 MB limit for secrets file
            if file_size > max_size:
                logger.error(f"Secrets file too large: {file_size} bytes (limit: {max_size} bytes)")
                return
                
            with open(secrets_file_path, 'r') as f:
                secrets_config = yaml.safe_load(f)
                
            if not secrets_config:
                logger.warning(f"Empty or invalid YAML in secrets file {secrets_file_path}")
                return
                
            # Determine mapping of environment variables (support top-level or nested 'environment')
            env_vars = None
            if isinstance(secrets_config, dict):
                if isinstance(secrets_config.get('environment'), dict):
                    # Backward-compatible format
                    env_vars = secrets_config['environment']
                    logger.debug(f"Using 'environment' section from secrets file {secrets_file_path}")
                else:
                    # Preferred top-level mapping
                    env_vars = secrets_config
                    logger.debug(f"Using top-level mapping from secrets file {secrets_file_path}")
            else:
                logger.warning(f"Secrets file {secrets_file_path} must be a YAML mapping at the top level")
                return

            if not env_vars:
                logger.warning(f"No environment variables found in secrets file {secrets_file_path}")
                return
            
            # Set environment variables from the secrets file
            secrets_loaded = 0
            for key, value in env_vars.items():
                if isinstance(key, str) and value is not None:
                    # Convert value to string for environment variable
                    environ[key] = str(value)
                    secrets_loaded += 1
                    logger.debug(f"Set environment variable {key} from secrets file")
                else:
                    logger.warning(f"Skipping invalid environment variable in secrets file: {key}={value}")
            
            logger.info(f"Successfully loaded {secrets_loaded} environment variables from secrets file {secrets_file_path}")
                
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML in secrets file {secrets_file_path}: {e}")
        except Exception as e:
            logger.error(f"Error loading secrets file {secrets_file_path}: {e}")
    
    # ------------------------------------------------------------------
    # Internal YAML loading helpers (deduplicate cloned safety logic)
    # ------------------------------------------------------------------
    def _safe_read_yaml(self, file_path: str, max_size: int, kind: str) -> Any | None:
        """Safely read a YAML file with consistent validation.

        Args:
            file_path: Path to the YAML file
            max_size: Maximum allowed file size in bytes
            kind: Short label for logging context (e.g. 'secrets', 'feeds')

        Returns:
            Parsed YAML (mapping/list/primitive) or None on failure.
        """
        try:
            if not path.isfile(file_path):
                logger.warning(f"{kind.capitalize()} file not found at {file_path}")
                return None
            if not access(file_path, R_OK):
                logger.error(f"No read permission for {kind} file at {file_path}")
                return None
            size = path.getsize(file_path)
            if size > max_size:
                logger.error(f"{kind.capitalize()} file too large: {size} bytes (limit: {max_size} bytes)")
                return None
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f)
            if not data:
                logger.warning(f"Empty or invalid YAML in {kind} file {file_path}")
                return None
            return data
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML in {kind} file {file_path}: {e}")
        except Exception as e:
            logger.error(f"Error loading {kind} file {file_path}: {e}")
        return None

    def _load_feed_sources(self) -> None:
        """Populate self.FEED_SOURCES from feeds.yaml.

        Uses _safe_read_yaml for validation; no return value (side-effects only).
        Idempotent and resilient: any failure results in an empty mapping.
        """
        feeds_path = self.FEEDS_CONFIG_PATH
        config_data = self._safe_read_yaml(feeds_path, 5 * 1024 * 1024, 'feeds')
        # Reset derived proxy configuration on reload
        self.PROXY_URL = None
        if not config_data:
            self.FEED_SOURCES = {}
            return

        proxy_section = config_data.get('proxy') if isinstance(config_data, dict) else None
        if isinstance(proxy_section, dict):
            proxy_url_value = proxy_section.get('url')
            if isinstance(proxy_url_value, str):
                normalized_proxy = proxy_url_value.strip()
                if normalized_proxy:
                    self.PROXY_URL = normalized_proxy
                    logger.info("Configured HTTP proxy for feed fetching via feeds.yaml")
            elif proxy_url_value is not None:
                logger.warning(f"Invalid proxy.url entry in {feeds_path}; ignoring proxy configuration")
        elif proxy_section not in (None, False):
            logger.warning(f"Proxy configuration in {feeds_path} must be a mapping with a url field")

        feeds_section = config_data.get('feeds') if isinstance(config_data, dict) else None
        thresholds_section = config_data.get('thresholds') if isinstance(config_data, dict) else {}
        if not isinstance(feeds_section, dict):
            logger.warning(f"No valid feeds found in {feeds_path}")
            self.FEED_SOURCES = {}
            return

        new_sources: Dict[str, str] = {}
        for feed_slug, feed_cfg in feeds_section.items():
            if isinstance(feed_cfg, dict) and 'url' in feed_cfg:
                new_sources[feed_slug] = feed_cfg['url']
                logger.debug(f"Loaded feed {feed_slug}: {feed_cfg['url']}")
            else:
                logger.warning(f"Skipping invalid feed configuration for '{feed_slug}': {feed_cfg}")

        self.FEED_SOURCES = new_sources
        logger.info(f"Successfully loaded {len(self.FEED_SOURCES)} feeds from {feeds_path}")

        # Load thresholds (time window & retention) with safe defaults
        try:
            tw_raw = None
            rd_raw = None
            initial_bootstrap_raw = None
            if isinstance(thresholds_section, dict):
                tw_raw = thresholds_section.get('time_window_hours')
                rd_raw = thresholds_section.get('retention_days')
                initial_bootstrap_raw = thresholds_section.get('initial_fetch_items')
            # Parse with fallback defaults
            self.TIME_WINDOW_HOURS = 48
            if tw_raw is not None:
                try:
                    tw_val = int(str(tw_raw).strip())
                    if tw_val >= 1:
                        self.TIME_WINDOW_HOURS = tw_val
                    else:
                        logger.warning(f"time_window_hours must be >=1; keeping default 48 (got {tw_raw})")
                except Exception:
                    logger.warning(f"Invalid time_window_hours value '{tw_raw}' in feeds.yaml; using default 48")
            self.RETENTION_DAYS = 7
            if rd_raw is not None:
                try:
                    rd_val = int(str(rd_raw).strip())
                    if rd_val >= 1:
                        self.RETENTION_DAYS = rd_val
                    else:
                        logger.warning(f"retention_days must be >=1; keeping default 7 (got {rd_raw})")
                except Exception:
                    logger.warning(f"Invalid retention_days value '{rd_raw}' in feeds.yaml; using default 7")
            self.INITIAL_FETCH_ITEM_LIMIT = 10
            if initial_bootstrap_raw is not None:
                try:
                    bootstrap_val = int(str(initial_bootstrap_raw).strip())
                    if bootstrap_val >= 0:
                        self.INITIAL_FETCH_ITEM_LIMIT = bootstrap_val
                    else:
                        logger.warning(
                            "initial_fetch_items must be >=0; keeping default 10 (got %s)",
                            initial_bootstrap_raw,
                        )
                except Exception:
                    logger.warning(
                        "Invalid initial_fetch_items value '%s' in feeds.yaml; using default 10",
                        initial_bootstrap_raw,
                    )
            logger.info(
                "Loaded thresholds: TIME_WINDOW_HOURS=%s RETENTION_DAYS=%s INITIAL_FETCH_ITEM_LIMIT=%s",
                self.TIME_WINDOW_HOURS,
                self.RETENTION_DAYS,
                self.INITIAL_FETCH_ITEM_LIMIT,
            )
        except Exception as e:
            # Never fail due to thresholds parsing
            self.TIME_WINDOW_HOURS = getattr(self, 'TIME_WINDOW_HOURS', 48)
            self.RETENTION_DAYS = getattr(self, 'RETENTION_DAYS', 7)
            self.INITIAL_FETCH_ITEM_LIMIT = getattr(self, 'INITIAL_FETCH_ITEM_LIMIT', 10)
            logger.warning(f"Failed to parse thresholds from feeds.yaml; using defaults (48h window / 7d retention): {e}")
    
    def reload_feed_sources(self):
        """Reload feed sources from configuration file."""
        logger.info("Reloading feed sources configuration")
        self._load_feed_sources()
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get a summary of current configuration for logging/debugging."""
        return {
            "database_path": self.DATABASE_PATH,
            "fetch_interval_minutes": self.FETCH_INTERVAL_MINUTES,
            "max_retries": self.MAX_RETRIES,
            "http_timeout": self.HTTP_TIMEOUT,
            "reader_mode_requests_per_minute": self.READER_MODE_REQUESTS_PER_MINUTE,
            "save_batch_size": self.SAVE_BATCH_SIZE,
            "reader_mode_concurrency": self.READER_MODE_CONCURRENCY,
            "feed_count": len(self.FEED_SOURCES),
            "force_refresh": self.FORCE_REFRESH_FEEDS,
            "secrets_file_configured": bool(environ.get("SECRETS_FILE")),
            "has_azure_endpoint": bool(self.AZURE_ENDPOINT),
            "has_openai_key": bool(self.OPENAI_API_KEY),
            "has_azure_storage": bool(self.AZURE_STORAGE_ACCOUNT and self.AZURE_STORAGE_KEY),
            # summarizer_max_tokens removed
            # "summarizer_temperature" removed (using model defaults)
        }

# Global configuration instance
config = Config()
