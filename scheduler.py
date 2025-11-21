#!/usr/bin/env python3
"""
Smart Feed Processing Scheduler

This module implements a flexible scheduler that can run the feed processing pipeline
based on time schedules defined in the feeds.yaml configuration file. It supports:

- Multiple scheduled times per day
- Flexible time format parsing (HH:MM, H:MM, etc.)
- Timezone handling with UTC as default
- Graceful error handling and recovery
- Proper sleep calculation to next scheduled time
- Status reporting and logging
"""

import asyncio
import yaml
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Optional, Dict, Any, Tuple
from pathlib import Path

from config import config, get_logger
from telemetry import init_telemetry, get_tracer, trace_span
from models import DatabaseQueue

# Module-specific logger
logger = get_logger("scheduler")

# Initialize telemetry for the scheduler subsystem
init_telemetry("feed-summarizer-scheduler")
_tracer = get_tracer("scheduler")

class ScheduleEntry:
    """Represents a single scheduled time entry."""
    
    def __init__(self, time_str: str):
        """Initialize schedule entry from time string.
        
        Args:
            time_str: Time in format "HH:MM", "H:MM", etc.
            
        Raises:
            ValueError: If time format is invalid
        """
        self.time_str = time_str
        self.time = self._parse_time(time_str)
    
    def _parse_time(self, time_str: str) -> time:
        """Parse time string into time object.
        
        Supports formats: "HH:MM", "H:MM", "6:30", "06:30", etc.
        """
        try:
            # Remove quotes and whitespace
            clean_time = str(time_str).strip().strip('"\'')
            
            # Split by colon
            parts = clean_time.split(':')
            if len(parts) != 2:
                raise ValueError(f"Time must be in HH:MM format, got: {time_str}")
            
            hour = int(parts[0])
            minute = int(parts[1])
            
            # Validate ranges
            if not (0 <= hour <= 23):
                raise ValueError(f"Hour must be 0-23, got: {hour}")
            if not (0 <= minute <= 59):
                raise ValueError(f"Minute must be 0-59, got: {minute}")
            
            return time(hour=hour, minute=minute)
            
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid time format '{time_str}': {e}")
    
    def next_occurrence(self, from_time: Optional[datetime] = None, tz: Optional[timezone] = None) -> datetime:
        """Get the next occurrence of this scheduled time in UTC.

        Args:
            from_time: Reference time (UTC or any tz-aware). Defaults to now (UTC).
            tz: The schedule timezone (ZoneInfo/UTC). Defaults to UTC.

        Returns:
            The next scheduled run as a UTC datetime.

        Notes:
            We convert the reference time to the schedule timezone, compute the next local
            occurrence, then return it converted back to UTC. This keeps internal scheduler
            logic uniformly in UTC while allowing human-friendly local times in config.
        """
        if tz is None:
            tz = timezone.utc
        if from_time is None:
            from_time = datetime.now(timezone.utc)
        # Normalize reference time to schedule timezone
        ref_local = from_time.astimezone(tz)
        candidate_local = datetime.combine(ref_local.date(), self.time, tzinfo=tz)
        if candidate_local <= ref_local:
            candidate_local = candidate_local + timedelta(days=1)
        # Return as UTC
        return candidate_local.astimezone(timezone.utc)
    
    def __str__(self) -> str:
        return f"ScheduleEntry({self.time_str})"
    
    def __repr__(self) -> str:
        return self.__str__()


class FeedScheduler:
    """Smart scheduler for feed processing pipeline."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize scheduler.
        
        Args:
            config_path: Path to feeds.yaml config file (default: uses config.FEEDS_CONFIG_PATH)
        """
        self.config_path = config_path or config.FEEDS_CONFIG_PATH
        self.schedule_entries = []  # type: List[ScheduleEntry]
        self.feed_schedules = {}    # type: Dict[str, List[ScheduleEntry]]
        # Timezone configuration (default from YAML, overridden by environment variable when present).
        # Start with feeds.yaml value (loaded later) or UTC, then let SCHEDULER_TIMEZONE override it.
        self.schedule_timezone_name = "UTC"
        try:
            # Apply environment override first if set
            if config.SCHEDULER_TIMEZONE:
                self.schedule_timezone_name = str(config.SCHEDULER_TIMEZONE)
            self.schedule_timezone = ZoneInfo(self.schedule_timezone_name)
        except Exception:
            logger.warning(f"Invalid timezone '{self.schedule_timezone_name}', falling back to UTC")
            self.schedule_timezone_name = "UTC"
            self.schedule_timezone = timezone.utc
        self._load_schedule()
        self.db: Optional[DatabaseQueue] = None
        # Cache of per-feed interval minutes from feeds.yaml
        self.feed_intervals: Dict[str, int] = {}
        self._load_feed_intervals()

    def _load_feed_intervals(self):
        """Parse per-feed interval settings from feeds.yaml (interval_minutes or refresh_interval_minutes)."""
        try:
            if not Path(self.config_path).exists():
                return
            with open(self.config_path, 'r') as f:
                cfg = yaml.safe_load(f) or {}
            feeds_cfg = cfg.get('feeds', {}) if isinstance(cfg, dict) else {}
            for slug, fc in (feeds_cfg.items() if isinstance(feeds_cfg, dict) else []):
                if isinstance(fc, dict):
                    val = fc.get('interval_minutes', fc.get('refresh_interval_minutes'))
                    try:
                        if val is not None:
                            minutes = int(val)
                            if minutes > 0:
                                self.feed_intervals[slug] = minutes
                    except Exception:
                        continue
            if self.feed_intervals:
                logger.info("Loaded per-feed interval minutes for: %s", ', '.join(sorted(self.feed_intervals.keys())))
        except Exception as e:
            logger.error(f"Error loading per-feed intervals: {e}")
    
    def _load_schedule(self):
        """Load schedule configuration from YAML file."""
        try:
            if not Path(self.config_path).exists():
                logger.warning(f"Config file not found: {self.config_path}")
                return
            
            with open(self.config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            
            if not config_data:
                logger.warning("Empty configuration file")
                return
            
            # Global schedule (optional)
            parsed_entries: List[ScheduleEntry] = []
            global_schedule = config_data.get('schedule')

            # Support two formats:
            # 1. Legacy list format:
            #    schedule:
            #      - time: "06:30"
            #      - time: "12:30"
            # 2. Mapping with timezone & times:
            #    schedule:
            #      timezone: Europe/Lisbon
            #      times:
            #        - "06:30"
            #        - "12:30"
            # or times: [ { time: "06:30" }, "12:30" ]
            if isinstance(global_schedule, list):
                raw_entries = global_schedule
            elif isinstance(global_schedule, dict):
                # Extract timezone override first
                tz_name = global_schedule.get('timezone') or global_schedule.get('tz')
                if tz_name:
                    try:
                        self.schedule_timezone = ZoneInfo(str(tz_name))
                        self.schedule_timezone_name = str(tz_name)
                        logger.info(f"Using schedule timezone from config: {self.schedule_timezone_name}")
                    except Exception:
                        logger.error(f"Invalid schedule timezone '{tz_name}' – keeping '{self.schedule_timezone_name}'")
                raw_entries = (global_schedule.get('times') or
                               global_schedule.get('entries') or
                               global_schedule.get('schedule') or [])
                if not isinstance(raw_entries, list):
                    logger.error("Schedule 'times' must be a list – ignoring")
                    raw_entries = []
            else:
                raw_entries = []

            for entry in raw_entries:
                try:
                    if isinstance(entry, dict) and 'time' in entry:
                        schedule_entry = ScheduleEntry(entry['time'])
                    elif isinstance(entry, str):
                        schedule_entry = ScheduleEntry(entry)
                    else:
                        logger.warning(f"Invalid schedule entry format: {entry}")
                        continue
                    parsed_entries.append(schedule_entry)
                    logger.debug(f"Loaded schedule entry: {schedule_entry}")
                except ValueError as e:
                    logger.error(f"Failed to parse schedule entry {entry}: {e}")
            self.schedule_entries = parsed_entries
            logger.info(f"Loaded {len(self.schedule_entries)} global schedule entries from {self.config_path}")

            # Per-feed schedule (optional) under feeds.<slug>.schedule: [{ time: "HH:MM" }, ...]
            self.feed_schedules = {}
            feeds_cfg = config_data.get('feeds', {}) if isinstance(config_data, dict) else {}
            if isinstance(feeds_cfg, dict):
                for slug, fc in feeds_cfg.items():
                    if isinstance(fc, dict) and isinstance(fc.get('schedule'), list):
                        entries: List[ScheduleEntry] = []
                        for e in fc['schedule']:
                            if isinstance(e, dict) and 'time' in e:
                                try:
                                    entries.append(ScheduleEntry(e['time']))
                                except ValueError as ex:
                                    logger.error(f"Failed to parse per-feed schedule time '{e}': {ex}")
                        if entries:
                            self.feed_schedules[slug] = entries
                if self.feed_schedules:
                    logger.info(f"Loaded per-feed schedules for: {', '.join(self.feed_schedules.keys())}")
            
            if self.schedule_entries:
                times_str = ", ".join(entry.time_str for entry in self.schedule_entries)
                logger.info(f"Scheduled times ({self.schedule_timezone_name}): {times_str}")
            
        except Exception as e:
            logger.error(f"Error loading schedule from {self.config_path}: {e}")
            self.schedule_entries = []
    
    def reload_schedule(self):
        """Reload schedule configuration from file."""
        logger.info("Reloading schedule configuration")
        self._load_schedule()
    
    def get_next_run_time(self, from_time: Optional[datetime] = None) -> Optional[datetime]:
        """Get the next scheduled run time.
        
        Args:
            from_time: Calculate from this time (default: now)
            
        Returns:
            Next scheduled datetime, or None if no schedule configured
        """
        if not self.schedule_entries:
            return None
        
        if from_time is None:
            from_time = datetime.now(timezone.utc)
        
        # Get next occurrence for each schedule entry
        next_times = [entry.next_occurrence(from_time, self.schedule_timezone) for entry in self.schedule_entries]
        # Return the earliest next time
        return min(next_times)

    async def get_next_run_event(self, from_time: Optional[datetime] = None) -> Tuple[Optional[datetime], Optional[List[str]]]:
        """Get the next scheduled run and which feeds are due.

        Combines time-based schedules (global and per-feed) with interval-based
        per-feed checks (interval_minutes/refresh_interval_minutes). Returns a
        tuple of (when, slugs). If slugs is None, it means a global run (all feeds).
        """
        if from_time is None:
            from_time = datetime.now(timezone.utc)

        candidates: List[Tuple[datetime, Optional[str]]] = []

        # Global schedule candidate(s)
        if self.schedule_entries:
            for entry in self.schedule_entries:
                candidates.append((entry.next_occurrence(from_time, self.schedule_timezone), None))

        # Per-feed schedule candidates (time-based)
        for slug, entries in self.feed_schedules.items():
            for entry in entries:
                candidates.append((entry.next_occurrence(from_time, self.schedule_timezone), slug))

        # Interval-based candidates: determine when each feed becomes due based on last_fetched
        # Only if we have DB available and at least one interval configured
        if self.db and self.feed_intervals:
            try:
                feeds = await self.db.execute('list_feeds')  # type: ignore[attr-defined]
            except Exception:
                feeds = []
            now_ts = int(from_time.timestamp())
            for feed in feeds:
                slug = feed.get('slug')
                if slug not in self.feed_intervals:
                    continue
                last = int(feed.get('last_fetched') or 0)
                interval_m = self.feed_intervals[slug]
                due_ts = (last or 0) + interval_m * 60
                # If never fetched (last == 0), mark as now
                if last == 0:
                    due_time = from_time
                else:
                    delta = max(0, due_ts - now_ts)
                    due_time = from_time + timedelta(seconds=delta)
                candidates.append((due_time, slug))

        if not candidates:
            return None, None

        # Find earliest time
        earliest_time = min(t for t, _ in candidates)
        # Gather all feeds with earliest time; if any None (global) is at earliest, it's a global run
        slugs_at_time = [slug for t, slug in candidates if t == earliest_time]
        if any(slug is None for slug in slugs_at_time):
            return earliest_time, None
        # Otherwise, return list of slugs due at this time (deduplicated)
        uniq_slugs = sorted({s for s in slugs_at_time if s is not None})
        return earliest_time, uniq_slugs

    def get_next_run_time_for_feed(self, slug: str, from_time: Optional[datetime] = None) -> Optional[datetime]:
        """Get the next scheduled run time for a specific feed.

        Uses per-feed schedule if defined; otherwise falls back to global schedule.
        """
        if from_time is None:
            from_time = datetime.now(timezone.utc)
        entries = self.feed_schedules.get(slug) or self.schedule_entries
        if not entries:
            return None
        return min(entry.next_occurrence(from_time, self.schedule_timezone) for entry in entries)
    
    def seconds_until_next_run(self, from_time: Optional[datetime] = None) -> Optional[float]:
        """Get seconds until the next scheduled run.
        
        Args:
            from_time: Calculate from this time (default: now)
            
        Returns:
            Seconds until next run, or None if no schedule configured
        """
        next_run = self.get_next_run_time(from_time)
        if next_run is None:
            return None
        
        if from_time is None:
            from_time = datetime.now(timezone.utc)
        
        delta = next_run - from_time
        return delta.total_seconds()
    
    def get_schedule_status(self) -> Dict[str, Any]:
        """Get current schedule status information.
        
        Returns:
            Dictionary with schedule status details
        """
        now = datetime.now(timezone.utc)
        next_run = self.get_next_run_time(now)
        seconds_until = self.seconds_until_next_run(now)
        
        status = {
            'current_time': now.isoformat(),
            'schedule_entries_count': len(self.schedule_entries),
            'schedule_times': [entry.time_str for entry in self.schedule_entries],
            'next_run_time': next_run.isoformat() if next_run else None,
            'seconds_until_next_run': seconds_until,
            'minutes_until_next_run': round(seconds_until / 60, 1) if seconds_until else None,
            'schedule_active': len(self.schedule_entries) > 0,
            'per_feed_schedules': {slug: [e.time_str for e in entries] for slug, entries in self.feed_schedules.items()},
            'schedule_timezone': self.schedule_timezone_name,
        }
        return status
    
    def print_schedule_status(self):
        """Print formatted schedule status."""
        status = self.get_schedule_status()
        
        print(f"\n🕐 Scheduler Status")
        print(f"⏰ Current time: {status['current_time']}")
        print(f"📅 Schedule entries: {status['schedule_entries_count']}")
        print(f"🌍 Timezone: {status.get('schedule_timezone', 'UTC')}")
        
        if status['schedule_active']:
            print(f"🎯 Scheduled times: {', '.join(status['schedule_times'])}")
            if status['next_run_time']:
                print(f"⏭️ Next run: {status['next_run_time']}")
                print(f"⏳ Time until next run: {status['minutes_until_next_run']:.1f} minutes")
        else:
            print("❌ No schedule configured")
    
    @trace_span("scheduler.main_loop", tracer_name="scheduler")
    async def run_scheduled_pipeline(self, orchestrator):
        """Run the pipeline in scheduled mode.
        
        Args:
            orchestrator: FeedProcessingOrchestrator instance to run
        """
        if not self.schedule_entries and not self.feed_schedules:
            logger.error("No schedule configured - cannot run in scheduled mode")
            logger.info("Please configure global or per-feed schedule times in feeds.yaml")
            return
        
        logger.info(f"🚀 Starting scheduled pipeline with {len(self.schedule_entries)} global times and {len(self.feed_schedules)} per-feed schedules")
        self.print_schedule_status()
        
        # Initialize DB queue to compute interval-based scheduling
        try:
            self.db = DatabaseQueue(config.DATABASE_PATH)
            await self.db.start()
        except Exception as e:
            logger.error(f"Failed to initialize DB for scheduler: {e}")
            self.db = None
        
        # Run immediately on startup if requested via environment variable
        run_immediately = config.SCHEDULER_RUN_IMMEDIATELY or config.FORCE_REFRESH_FEEDS
        if run_immediately:
            logger.info("🎬 Running pipeline immediately on startup (SCHEDULER_RUN_IMMEDIATELY=true or FORCE_REFRESH_FEEDS=true) — publishing disabled")
            # Warm start: fetch and summarize only; defer publishing to the first scheduled GLOBAL run
            await orchestrator.run_pipeline(publish_content=False)
        
        # Main scheduling loop
        while True:
            try:
                next_time, slugs = await self.get_next_run_event()  # type: ignore[arg-type]
                if next_time is None:
                    logger.error("No next run time calculated - stopping scheduler")
                    break
                
                # Add a small buffer to ensure we don't wake up too early
                now = datetime.now(timezone.utc)
                seconds_until = (next_time - now).total_seconds()
                sleep_time = max(1, seconds_until + 1)
                minutes_until = sleep_time / 60
                
                if slugs is None:
                    logger.info(f"😴 Sleeping {minutes_until:.1f} minutes until next GLOBAL run (timezone: {self.schedule_timezone_name})")
                else:
                    logger.info(f"😴 Sleeping {minutes_until:.1f} minutes until next per-feed run for: {', '.join(slugs)} (timezone: {self.schedule_timezone_name})")
                
                # Sleep until next scheduled time (decorated span)
                await self._sleep_until(next_time, sleep_time, slugs)
                
                # Run the pipeline
                if slugs is None:
                    logger.info("⏰ Starting scheduled GLOBAL pipeline run")
                else:
                    logger.info(f"⏰ Starting scheduled per-feed pipeline run for: {', '.join(slugs)}")
                start_time = datetime.now(timezone.utc)
                success, duration = await self._run_pipeline_with_span(orchestrator, next_time, slugs)
                
                if success:
                    logger.info(f"✅ Scheduled pipeline run completed successfully in {duration:.1f}s")
                else:
                    logger.error(f"❌ Scheduled pipeline run failed after {duration:.1f}s")
                
            except asyncio.CancelledError:
                logger.info("📶 Scheduler cancelled - shutting down")
                break
            except Exception as e:
                logger.error(f"💥 Error in scheduled pipeline run: {e}")
                # Continue running despite errors
                await asyncio.sleep(60)  # Wait 1 minute before next attempt

        # Clean up DB if started
        if self.db:
            try:
                await self.db.stop()
            except Exception:
                pass

    @trace_span(
        "scheduler.sleep",
        tracer_name="scheduler",
        attr_from_args=lambda self, next_time, sleep_time, slugs: {
            "sleep.seconds": float(sleep_time),
            "scheduled.at": next_time.isoformat(),
            "run.global": slugs is None,
            "feed.only_slugs": ",".join(slugs) if slugs else "",
        },
    )
    async def _sleep_until(self, next_time: datetime, sleep_time: float, slugs: Optional[list]):
        await asyncio.sleep(sleep_time)

    @trace_span(
        "scheduler.pipeline_run",
        tracer_name="scheduler",
        attr_from_args=lambda self, orchestrator, next_time, slugs: {
            "scheduled.at": next_time.isoformat(),
            "run.global": slugs is None,
            "feed.only_slugs": ",".join(slugs) if slugs else "",
        },
    )
    async def _run_pipeline_with_span(self, orchestrator, next_time: datetime, slugs: Optional[list]):
        start_time = datetime.now(timezone.utc)
        # Always publish on scheduled runs (unified publish path)
        publish_content = True
        success = await orchestrator.run_pipeline(
            publish_content=publish_content,
            only_slugs=slugs,
            enable_azure_upload=True,
        )
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        return success, duration


# Convenience function for external use
def create_scheduler(config_path: Optional[str] = None) -> FeedScheduler:
    """Create a FeedScheduler instance.
    
    Args:
        config_path: Path to feeds.yaml config file
        
    Returns:
        Configured FeedScheduler instance
    """
    return FeedScheduler(config_path)
