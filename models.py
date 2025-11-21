#!/usr/bin/env python3
"""
Database models and operations for the Feed Fetcher.

This module contains all database-related classes and functions,
providing a clean separation between data access and business logic.
"""

from os import path, access, R_OK
from time import time
import json
from sqlite3 import connect, Row, Error
from asyncio import Queue, create_task, wait_for, TimeoutError, CancelledError, Event, Lock
from uuid import uuid4
from typing import Dict, List, Optional, Set, Any, Tuple
import yaml

# Import config for unified logging
from config import config, get_logger
from telemetry import get_tracer, trace_span

# Module-specific logger
logger = get_logger("models")
_tracer = get_tracer("db")


def initialize_database(conn) -> None:
    """Initialize the database with the defined schema from SQL file."""
    # Check if the database is new or empty
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='feeds'")
        feeds_table_exists = cursor.fetchone() is not None
        
        if not feeds_table_exists:
            logger.info("Database is new or empty. Initializing schema.")
            
            # Read and execute schema from external file
            schema_sql = _read_schema_file()
            cursor.executescript(schema_sql)
            conn.commit()
            logger.info("Database schema initialized successfully")
        else:
            logger.info("Database already exists with proper schema")
            # Run any necessary migrations
            _run_migrations(conn)
            
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise
    finally:
        cursor.close()


def _run_migrations(conn) -> None:
    """Run any necessary database migrations."""
    cursor = conn.cursor()
    
    try:
        # Migration 1: Add published_date column to summaries table if it doesn't exist
        cursor.execute("PRAGMA table_info(summaries)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'published_date' not in columns:
            logger.info("Adding published_date column to summaries table")
            cursor.execute("ALTER TABLE summaries ADD COLUMN published_date INTEGER")
            conn.commit()
            logger.info("Migration completed: added published_date column")
        
        # Migration 2: Add bulletins and bulletin_summaries tables if they don't exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bulletins'")
        bulletins_exists = cursor.fetchone() is not None
        
        if not bulletins_exists:
            logger.info("Creating bulletins table")
            cursor.execute("""
                CREATE TABLE bulletins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_name TEXT NOT NULL,
                    session_key TEXT NOT NULL,
                    created_date INTEGER NOT NULL,
                    title TEXT,
                    introduction TEXT,
                    summary_count INTEGER DEFAULT 0,
                    feed_slugs TEXT,
                    UNIQUE(group_name, session_key)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE bulletin_summaries (
                    bulletin_id INTEGER,
                    summary_id INTEGER,
                    PRIMARY KEY (bulletin_id, summary_id),
                    FOREIGN KEY (bulletin_id) REFERENCES bulletins(id) ON DELETE CASCADE,
                    FOREIGN KEY (summary_id) REFERENCES summaries(id) ON DELETE CASCADE
                )
            """)
            
            conn.commit()
            logger.info("Migration completed: created bulletins and bulletin_summaries tables")
        else:
            # Migration 3: Add title column to bulletins if it doesn't exist
            try:
                cursor.execute("PRAGMA table_info(bulletins)")
                bcols = [row[1] for row in cursor.fetchall()]
                if 'title' not in bcols:
                    logger.info("Adding title column to bulletins table")
                    cursor.execute("ALTER TABLE bulletins ADD COLUMN title TEXT")
                    conn.commit()
                    logger.info("Migration completed: added title column to bulletins")
            except Exception as e:
                logger.warning(f"Could not add title column to bulletins (may already exist): {e}")
            
    except Exception as e:
        logger.error(f"Error running migrations: {e}")
        raise
    finally:
        cursor.close()


def _read_schema_file() -> str:
    """Read the schema from the SQL file."""
    from config import config
    schema_path = config.SCHEMA_FILE_PATH
    
    try:
        # Check if file exists and is accessible before attempting to read it
        if not path.isfile(schema_path):
            raise FileNotFoundError(f"Schema file not found at {schema_path}")
        
        # Check if we have read permissions
        if not access(schema_path, R_OK):
            raise PermissionError(f"No read permission for schema file at {schema_path}")
            
        # Check file size to prevent reading extremely large files
        file_size = path.getsize(schema_path)
        max_size = config.SCHEMA_FILE_SIZE_LIMIT_MB * 1024 * 1024
        if file_size > max_size:
            raise ValueError(f"Schema file too large: {file_size} bytes (limit: {max_size} bytes)")
            
        with open(schema_path, 'r') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading schema file: {e}")
        raise


class DatabaseQueue:
    """A queue for database operations to ensure thread safety."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.queue = Queue()
        self.results: Dict[str, Dict] = {}
        self.events: Dict[str, Event] = {}
        self.conn = None
        self.running = False
        self.worker_task = None
    
    async def start(self) -> None:
        """Start the database worker."""
        if self.running:
            return
        
        self.running = True
        self.worker_task = create_task(self._worker())
        logger.info("Database worker started")
    
    async def stop(self) -> None:
        """Stop the database worker."""
        if not self.running:
            return
        
        self.running = False
        if self.worker_task:
            self.worker_task.cancel()
            try:
                await self.worker_task
            except CancelledError:
                pass
            
        if self.conn:
            self.conn.close()
            self.conn = None
            
        # Clean up any remaining events and results to prevent memory leaks
        for event in self.events.values():
            event.set()
        self.events.clear()
        self.results.clear()
            
        logger.info("Database worker stopped")
    
    async def _worker(self) -> None:
        """Worker coroutine processing database operations."""
        # Check if the database file exists
        db_exists = path.isfile(self.db_path)
        if not db_exists:
            logger.info(f"Database file {self.db_path} does not exist. A new database will be created.")
        else:
            logger.info(f"Using existing database at {self.db_path}")
            
        # Connect to database in this thread
        self.conn = connect(self.db_path)
        self.conn.row_factory = Row
        
        # Initialize tables using external schema
        initialize_database(self.conn)
        
        while self.running:
            try:
                # Get the next operation from the queue with timeout
                try:
                    operation_id, operation_name, params = await wait_for(self.queue.get(), timeout=1.0)
                except TimeoutError:
                    continue
                
                try:
                    # Execute the appropriate database operation
                    if hasattr(self, operation_name):
                        method = getattr(self, operation_name)
                        result = method(**params)
                        self.results[operation_id] = {"result": result}
                    else:
                        self.results[operation_id] = {"error": f"Unknown operation: {operation_name}"}
                except Exception as e:
                    logger.error(f"Database operation error in {operation_name}: {e}")
                    self.results[operation_id] = {"error": str(e)}
                finally:
                    # Signal that the operation is complete
                    if operation_id in self.events:
                        self.events[operation_id].set()
                    self.queue.task_done()
            
            except CancelledError:
                logger.info("Database worker cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error in database worker: {e}")
    
    @trace_span(
        "db.execute",
        tracer_name="db",
        static_attrs={"db.system": "sqlite"},
        attr_from_args=lambda self, operation_name, **params: {
            "db.operation": operation_name,
            "db.params.keys": ",".join(sorted(params.keys())) if params else "",
        },
    )
    async def execute(self, operation_name: str, **params) -> Any:
        """Execute a database operation."""
        # Generate unique ID for this operation
        operation_id = str(uuid4())
        
        # Create an event for this operation
        event = Event()
        self.events[operation_id] = event
        
        try:
            # Put operation in queue
            await self.queue.put((operation_id, operation_name, params))
            
            # Wait for the event to be signaled (operation complete)
            await event.wait()
            
            # Get and remove result
            result = self.results.pop(operation_id)
            
            # Check for error
            if "error" in result:
                raise Exception(result["error"])
                
            return result["result"]
        finally:
            # Clean up the event to prevent memory leaks
            self.events.pop(operation_id, None)
    
    # Feed Management Operations
    def register_feed(self, slug: str, url: str) -> bool:
        """Register a feed in the database."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT OR IGNORE INTO feeds (slug, url, last_fetched) VALUES (?, ?, 0)",
                (slug, url)
            )
            self.conn.commit()
            return True
        except Error as e:
            logger.error(f"Error registering feed {slug}: {e}")
            return False
    
    def get_feed_id(self, slug: str) -> Optional[int]:
        """Get the ID of a feed by its slug."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id FROM feeds WHERE slug = ?", (slug,))
            result = cursor.fetchone()
            return result['id'] if result else None
        except Error as e:
            logger.error(f"Error getting feed ID for {slug}: {e}")
            return None
    
    def update_feed_title(self, feed_id: int, title: str) -> bool:
        """Update the title of a feed."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("UPDATE feeds SET title = ? WHERE id = ?", (title, feed_id))
            self.conn.commit()
            return True
        except Error as e:
            logger.error(f"Error updating feed title for ID {feed_id}: {e}")
            return False
    
    def update_last_fetched(self, feed_id: int) -> bool:
        """Update the last_fetched timestamp for a feed."""
        try:
            current_time = int(time())
            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE feeds SET last_fetched = ? WHERE id = ?", 
                (current_time, feed_id)
            )
            self.conn.commit()
            return True
        except Error as e:
            logger.error(f"Error updating last_fetched for feed ID {feed_id}: {e}")
            return False
    
    def get_feed_last_fetched(self, feed_id: int) -> int:
        """Get the last_fetched timestamp of a feed by its ID."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT last_fetched FROM feeds WHERE id = ?", (feed_id,))
            result = cursor.fetchone()
            return result['last_fetched'] if result else 0
        except Error as e:
            logger.error(f"Error getting last_fetched for feed ID {feed_id}: {e}")
            return 0

    def list_feeds(self) -> List[Dict[str, Any]]:
        """List all feeds with id, slug, and last_fetched.

        Returns:
            A list of dicts: { 'id': int, 'slug': str, 'last_fetched': int }
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id, slug, last_fetched FROM feeds")
            rows = cursor.fetchall()
            return [{
                'id': row[0],
                'slug': row[1],
                'last_fetched': row[2] if row[2] is not None else 0
            } for row in rows]
        except Error as e:
            logger.error(f"Error listing feeds: {e}")
            return []
    
    # Item Management Operations
    def save_items(self, feed_id: int, entries_data: List[Dict[str, Any]]) -> int:
        """Save feed items to the database."""
        new_items = 0
        try:
            cursor = self.conn.cursor()
            
            for entry_data in entries_data:
                # Insert the item, ignoring if URL already exists
                try:
                    cursor.execute('''
                    INSERT OR IGNORE INTO items (feed_id, title, url, guid, body, date) 
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        feed_id, 
                        entry_data['title'],
                        entry_data['url'],
                        entry_data['guid'],
                        entry_data['body'],
                        entry_data['date']
                    ))
                    
                    if cursor.rowcount > 0:
                        new_items += 1
                        
                except Error as e:
                    logger.error(f"Error inserting item {entry_data['url']}: {e}")
                    continue
                    
            self.conn.commit()
            return new_items
            
        except Exception as e:
            logger.error(f"Error saving items for feed ID {feed_id}: {e}")
            return 0
    
    def check_existing_guids(self, feed_id: int, guids: List[str]) -> Set[str]:
        """Check which GUIDs already exist in the database for this feed."""
        try:
            if not guids:
                return set()
            
            cursor = self.conn.cursor()
            
            # Create a query with the right number of placeholders
            placeholders = ','.join(['?' for _ in guids])
            query = f"SELECT guid FROM items WHERE feed_id = ? AND guid IN ({placeholders})"
            
            cursor.execute(query, [feed_id] + list(guids))
            existing_guids = {row[0] for row in cursor.fetchall()}
            
            return existing_guids
            
        except Error as e:
            logger.error(f"Error checking existing GUIDs for feed ID {feed_id}: {e}")
            return set()

    def check_existing_urls(self, urls: List[str]) -> Set[str]:
        """Check which URLs already exist globally (across all feeds).

        This enables cross-feed deduplication for sources that syndicate
        the same articles in multiple category feeds (e.g. The Register).

        Args:
            urls: List of candidate item URLs.

        Returns:
            Set of URLs already present in the items table.
        """
        try:
            if not urls:
                return set()
            cursor = self.conn.cursor()
            placeholders = ','.join(['?' for _ in urls])
            query = f"SELECT url FROM items WHERE url IN ({placeholders})"
            cursor.execute(query, urls)
            existing = {row[0] for row in cursor.fetchall()}
            return existing
        except Error as e:
            logger.error(f"Error checking existing URLs: {e}")
            return set()
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def count_items(self) -> int:
        """Return total number of rows in items table (utility for tests)."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM items")
            result = cursor.fetchone()
            return int(result[0]) if result else 0
        except Error as e:
            logger.error(f"Error counting items: {e}")
            return 0
        finally:
            try:
                cursor.close()
            except Exception:
                pass
    
    # HTTP Header Caching Operations
    def get_feed_etag(self, feed_id: int) -> Optional[str]:
        """Get the stored ETag for a feed."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT etag FROM feeds WHERE id = ?", (feed_id,))
            result = cursor.fetchone()
            return result['etag'] if result and 'etag' in result else None
        except Error as e:
            logger.error(f"Error getting etag for feed ID {feed_id}: {e}")
            return None
    
    def get_feed_last_modified(self, feed_id: int) -> Optional[str]:
        """Get the stored Last-Modified header for a feed."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT last_modified FROM feeds WHERE id = ?", (feed_id,))
            result = cursor.fetchone()
            return result['last_modified'] if result and 'last_modified' in result else None
        except Error as e:
            logger.error(f"Error getting last_modified for feed ID {feed_id}: {e}")
            return None
    
    def update_feed_headers(self, feed_id: int, etag: Optional[str] = None, last_modified: Optional[str] = None) -> bool:
        """Update the HTTP headers for a feed."""
        try:
            current_time = int(time())
            cursor = self.conn.cursor()
            
            # Build the update statement dynamically based on which headers are provided
            update_parts = ["last_fetched = ?"]
            params = [current_time]
            
            if etag is not None:
                update_parts.append("etag = ?")
                params.append(etag)
                
            if last_modified is not None:
                update_parts.append("last_modified = ?")
                params.append(last_modified)
                
            params.append(feed_id)  # For the WHERE clause
            
            cursor.execute(
                f"UPDATE feeds SET {', '.join(update_parts)} WHERE id = ?", 
                tuple(params)
            )
            self.conn.commit()
            return True
        except Error as e:
            logger.error(f"Error updating headers for feed ID {feed_id}: {e}")
            return False
    
    # Error Tracking Operations
    def update_feed_error(self, feed_id: int, error_count: int, last_error: Optional[str] = None) -> bool:
        """Update error tracking for a feed."""
        try:
            cursor = self.conn.cursor()
            
            current_time = int(time())
            cursor.execute(
                "UPDATE feeds SET error_count = ?, last_error = ?, last_fetched = ? WHERE id = ?",
                (error_count, last_error, current_time, feed_id)
            )
            self.conn.commit()
            return True
            
        except Error as e:
            logger.error(f"Error updating feed error info for feed ID {feed_id}: {e}")
            return False

    def get_feed_error_info(self, feed_id: int) -> Dict[str, Any]:
        """Get error tracking information for a feed."""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("SELECT error_count, last_error FROM feeds WHERE id = ?", (feed_id,))
            result = cursor.fetchone()
            if result:
                return {'error_count': result[0] or 0, 'last_error': result[1]}
            return {'error_count': 0, 'last_error': None}
            
        except Error as e:
            logger.error(f"Error retrieving feed error info for feed ID {feed_id}: {e}")
            return {'error_count': 0, 'last_error': None}

    # Summary Publishing Operations
    def mark_summaries_as_published(self, summary_ids: List[int]) -> int:
        """Mark summaries as published by setting the published_date timestamp."""
        if not summary_ids:
            return 0
            
        try:
            cursor = self.conn.cursor()
            current_time = int(time())
            
            # Create placeholders for the IN clause
            placeholders = ','.join(['?' for _ in summary_ids])
            
            # Update published_date for all specified summary IDs
            cursor.execute(
                f"UPDATE summaries SET published_date = ? WHERE id IN ({placeholders})",
                [current_time] + summary_ids
            )
            
            rows_affected = cursor.rowcount
            self.conn.commit()
            
            logger.debug(f"Database: marked {rows_affected} summaries as published")
            return rows_affected
            
        except Error as e:
            logger.error(f"Error marking summaries as published {summary_ids}: {e}")
            return 0

    def query_summaries_for_feeds(self, feed_slugs: List[str], limit: int = 50) -> List[Dict[str, Any]]:
        """Query summaries for specific feeds for HTML publishing.
        
        Args:
            feed_slugs: List of feed slugs to query
            limit: Maximum number of summaries to return
            
        Returns:
            List of dictionaries with summary details
        """
        if not feed_slugs:
            return []
            
        cursor = None
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.row_factory = Row
                
                # Create placeholders for the IN clause
                placeholders = ','.join(['?' for _ in feed_slugs])
                
                # Query for unpublished summaries from specified feeds, ordered by original item date
                query = f"""
                    SELECT 
                        s.id,
                        s.summary_text,
                        s.topic,
                        s.generated_date,
                        s.published_date,
                        i.title as item_title,
                        i.url as item_url,
                        i.date as item_date,
                        f.title as feed_title,
                        f.slug as feed_slug
                    FROM summaries s
                    JOIN items i ON s.id = i.id
                    JOIN feeds f ON i.feed_id = f.id
                    WHERE f.slug IN ({placeholders}) 
                    AND s.summary_text IS NOT NULL
                    AND s.summary_text != ''
                    AND s.published_date IS NULL
                    ORDER BY i.date DESC
                    LIMIT ?
                """
                
                cursor.execute(query, feed_slugs + [limit])
                rows = cursor.fetchall()
                
                # Convert to list of dictionaries
                summaries = []
                for row in rows:
                    summaries.append({
                        'id': row['id'],
                        'summary_text': row['summary_text'],
                        'topic': row['topic'],
                        'generated_date': row['generated_date'],
                        'published_date': row['published_date'],
                        'item_title': row['item_title'],
                        'item_url': row['item_url'],
                        'item_date': row['item_date'],
                        'feed_title': row['feed_title'],
                        'feed_slug': row['feed_slug']
                    })
                
                logger.debug(f"Found {len(summaries)} summaries for feeds: {feed_slugs}")
                return summaries
                
        except Error as e:
            logger.error(f"Error querying summaries for feeds {feed_slugs}: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def query_unpublished_summaries_for_feeds(self, feed_slugs: List[str], limit: int = 50) -> List[Dict[str, Any]]:
        """Query unpublished summaries for specific feeds.
        
        Args:
            feed_slugs: List of feed slugs to query
            limit: Maximum number of summaries to return
            
        Returns:
            List of dictionaries with unpublished summary details
        """
        if not feed_slugs:
            return []
            
        cursor = None
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.row_factory = Row
                
                # Create placeholders for the IN clause
                placeholders = ','.join(['?' for _ in feed_slugs])
                
                # Query for unpublished summaries from specified feeds
                query = f"""
                    SELECT 
                        s.id,
                        s.summary_text,
                        s.topic,
                        s.generated_date,
                        s.published_date,
                        i.title as item_title,
                        i.url as item_url,
                        i.date as item_date,
                        f.title as feed_title,
                        f.slug as feed_slug
                    FROM summaries s
                    JOIN items i ON s.id = i.id
                    JOIN feeds f ON i.feed_id = f.id
                    WHERE f.slug IN ({placeholders}) 
                    AND s.summary_text IS NOT NULL
                    AND s.summary_text != ''
                    AND s.published_date IS NULL
                    ORDER BY i.date DESC
                    LIMIT ?
                """
                
                cursor.execute(query, feed_slugs + [limit])
                rows = cursor.fetchall()
                
                # Convert to list of dictionaries
                summaries = []
                for row in rows:
                    summaries.append({
                        'id': row['id'],
                        'summary_text': row['summary_text'],
                        'topic': row['topic'],
                        'generated_date': row['generated_date'],
                        'published_date': row['published_date'],
                        'item_title': row['item_title'],
                        'item_url': row['item_url'],
                        'item_date': row['item_date'],
                        'feed_title': row['feed_title'],
                        'feed_slug': row['feed_slug']
                    })
                
                logger.debug(f"Found {len(summaries)} unpublished summaries for feeds: {feed_slugs}")
                return summaries
                
        except Error as e:
            logger.error(f"Error querying unpublished summaries for feeds {feed_slugs}: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def query_published_summaries_by_date(self, feed_slugs: List[str], cutoff_time: int) -> List[Dict[str, Any]]:
        """Query published summaries for specific feeds, grouped by publication date.
        
        Args:
            feed_slugs: List of feed slugs to query
            cutoff_time: Unix timestamp - only return summaries published after this time
            
        Returns:
            List of dictionaries with summary details including published_date
        """
        if not feed_slugs:
            return []
            
        cursor = None
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.row_factory = Row
                
                # Create placeholders for the IN clause
                placeholders = ','.join(['?' for _ in feed_slugs])
                
                # Query for published summaries from specified feeds after cutoff time
                query = f"""
                    SELECT 
                        s.id,
                        s.summary_text,
                        s.topic,
                        s.generated_date,
                        s.published_date,
                        i.title as item_title,
                        i.url as item_url,
                        i.date as item_date,
                        f.title as feed_title,
                        f.slug as feed_slug
                    FROM summaries s
                    JOIN items i ON s.id = i.id
                    JOIN feeds f ON i.feed_id = f.id
                    WHERE f.slug IN ({placeholders}) 
                    AND s.summary_text IS NOT NULL
                    AND s.summary_text != ''
                    AND s.published_date IS NOT NULL
                    AND s.published_date >= ?
                    ORDER BY s.published_date DESC, i.date DESC
                """
                
                cursor.execute(query, feed_slugs + [cutoff_time])
                rows = cursor.fetchall()
                
                # Convert to list of dictionaries
                summaries = []
                for row in rows:
                    summaries.append({
                        'id': row['id'],
                        'summary_text': row['summary_text'],
                        'topic': row['topic'],
                        'generated_date': row['generated_date'],
                        'published_date': row['published_date'],
                        'item_title': row['item_title'],
                        'item_url': row['item_url'],
                        'item_date': row['item_date'],
                        'feed_title': row['feed_title'],
                        'feed_slug': row['feed_slug']
                    })
                
                logger.debug(f"Found {len(summaries)} published summaries for feeds: {feed_slugs}")
                return summaries
                
        except Error as e:
            logger.error(f"Error querying published summaries by date for feeds {feed_slugs}: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    # Database Maintenance Operations
    def expire_old_entries(self, expiration_days: int) -> int:
        """Delete items and their summaries that are older than the specified number of days.
        
        Args:
            expiration_days: Number of days to keep items (items older than this will be deleted)
            
        Returns:
            Number of items deleted
        """
        if expiration_days <= 0:
            logger.warning("Invalid expiration_days value, skipping expiration")
            return 0
            
        cursor = None
        try:
            # Calculate the cutoff timestamp (items older than this will be deleted)
            cutoff_timestamp = int(time()) - (expiration_days * 24 * 60 * 60)
            
            cursor = self.conn.cursor()
            
            # First, count how many items will be deleted (for logging)
            cursor.execute("SELECT COUNT(*) FROM items WHERE date < ?", (cutoff_timestamp,))
            items_to_delete = cursor.fetchone()[0]
            
            if items_to_delete == 0:
                logger.debug(f"No items older than {expiration_days} days found")
                return 0
            
            # Delete summaries first (due to foreign key constraint)
            cursor.execute("""
                DELETE FROM summaries 
                WHERE id IN (
                    SELECT id FROM items WHERE date < ?
                )
            """, (cutoff_timestamp,))
            summaries_deleted = cursor.rowcount
            
            # Then delete the items
            cursor.execute("DELETE FROM items WHERE date < ?", (cutoff_timestamp,))
            items_deleted = cursor.rowcount
            
            # Commit the transaction
            self.conn.commit()
            
            logger.info(f"Database maintenance: deleted {items_deleted} items and {summaries_deleted} summaries older than {expiration_days} days")
            return items_deleted
            
        except Error as e:
            logger.error(f"Error during database maintenance (expiring old entries): {e}")
            if self.conn:
                self.conn.rollback()
            return 0
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def prune_items_per_feed(self, feed_id: int, max_items: int) -> int:
        """Retain only the newest `max_items` items for a feed.

        Deletes older items (and their summaries) beyond the count window, preserving
        recent history to prevent re-ingesting date-less items as "new" while bounding
        database growth.

        Args:
            feed_id: Feed ID whose items to prune.
            max_items: Maximum number of newest items to keep.

        Returns:
            Number of items deleted.
        """
        if max_items <= 0:
            return 0
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id FROM items WHERE feed_id = ? ORDER BY date DESC", (feed_id,))
            rows = cursor.fetchall()
            total = len(rows)
            if total <= max_items:
                return 0
            # Determine items to delete (older tail beyond max_items)
            ids_to_delete = [r[0] for r in rows[max_items:]]
            if not ids_to_delete:
                return 0
            # Delete summaries first
            placeholders = ','.join(['?' for _ in ids_to_delete])
            cursor.execute(f"DELETE FROM summaries WHERE id IN ({placeholders})", ids_to_delete)
            # Delete items
            cursor.execute(f"DELETE FROM items WHERE id IN ({placeholders})", ids_to_delete)
            deleted = cursor.rowcount
            self.conn.commit()
            logger.info(
                "Pruned %d old items for feed_id=%d (kept %d of %d)",
                deleted,
                feed_id,
                max_items,
                total,
            )
            return deleted
        except Error as e:
            logger.error(f"Error pruning items for feed ID {feed_id}: {e}")
            try:
                self.conn.rollback()
            except Exception:
                pass
            return 0
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def query_raw_feeds(self, slugs: List[str], cutoff_age_hours: Optional[int] = None) -> List[Dict[str, Any]]:
        """Query unsummarized items for specified feed slugs.
        
        Args:
            slugs: List of feed slugs to query
            
        Returns:
            List of dictionaries with item details for summarization
        """
        if not slugs:
            return []
            
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.row_factory = Row
            
            # Create placeholders for the IN clause
            placeholders = ','.join(['?' for _ in slugs])
            
            # Optional age cutoff (time window filtering)
            params: List[Any] = list(slugs)
            age_clause = ""
            if cutoff_age_hours is not None and cutoff_age_hours > 0:
                try:
                    cutoff_ts = int(time()) - int(cutoff_age_hours * 3600)
                    age_clause = " AND i.date >= ?"
                    params.append(cutoff_ts)
                except Exception:
                    age_clause = ""
            # Query for items that don't have summaries yet and (optionally) within time window
            # Dynamic limit from configuration (SUMMARY_WINDOW_ITEMS) injected at call site.
            limit_items = getattr(self, 'SUMMARY_WINDOW_ITEMS_OVERRIDE', 50)
            if not isinstance(limit_items, int) or limit_items <= 0:
                limit_items = 50
            query = f"""
                SELECT
                    i.url as url,
                    i.title as title,
                    i.body as body,
                    i.id as id,
                    strftime('%Y-%m-%dT%H:%M:%SZ', i.date, 'unixepoch') as date,
                    i.date as pubdate,
                    f.url as feed_url,
                    f.title as feed_title
                FROM feeds f
                JOIN items i ON i.feed_id = f.id
                LEFT JOIN summaries s ON i.id = s.id
                WHERE s.id IS NULL AND f.slug IN ({placeholders}){age_clause}
                ORDER BY i.date DESC
                LIMIT ?
            """
            cursor.execute(query, params + [limit_items])
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            items = []
            for row in rows:
                items.append({
                    'url': row['url'],
                    'title': row['title'],
                    'body': row['body'],
                    'id': row['id'],
                    'date': row['date'],
                    'pubdate': row['pubdate'],
                    'feed_url': row['feed_url'],
                    'feed_title': row['feed_title']
                })
            
            logger.debug("Found %d unsummarized items for feeds: %s", len(items), slugs)
            if len(items) > 0:
                logger.info("Processing %d unsummarized items from feeds: %s", len(items), slugs)
            return items
            
        except Error as e:
            logger.error(f"Error querying raw feeds for {slugs}: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def verify_and_mark_as_summarized(self, ids: List[int], summaries: Dict[int, Tuple[str, str]]) -> int:
        """
        Verify that the given item IDs exist and mark them as summarized with their summaries.
        
        Args:
            ids: List of item IDs to verify and mark as summarized
            summaries: Dictionary mapping item IDs to (summary_text, topic) tuples
            
        Returns:
            Number of items successfully marked as summarized
        """
        cursor = None
        try:
            cursor = self.conn.cursor()
            
            # First, verify which IDs actually exist in the items table
            placeholders = ','.join(['?' for _ in ids])
            query = f"""
                SELECT id FROM items 
                WHERE id IN ({placeholders})
            """
            
            cursor.execute(query, ids)
            existing_ids = [row[0] for row in cursor.fetchall()]
            
            if not existing_ids:
                logger.warning(f"None of the provided IDs {ids} exist in the items table")
                return 0
            
            # Insert summaries for existing IDs
            insert_count = 0
            for item_id in existing_ids:
                summary_info = summaries.get(item_id)
                if summary_info:
                    summary_text, topic = summary_info
                    try:
                        cursor.execute("""
                            INSERT OR REPLACE INTO summaries (id, summary_text, topic, generated_date)
                            VALUES (?, ?, ?, ?)
                        """, (item_id, summary_text, topic, int(time())))
                        insert_count += 1
                    except Error as e:
                        logger.error(f"Error inserting summary for item {item_id}: {e}")
                        continue
            
            self.conn.commit()
            logger.debug(f"Successfully marked {insert_count} items as summarized")
            return insert_count
            
        except Error as e:
            logger.error(f"Error verifying and marking items as summarized: {e}")
            return 0
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def get_feed_by_slug(self, slug: str) -> Optional[Dict[str, Any]]:
        """Return basic feed metadata for a given slug.

        Args:
            slug: The feed slug

        Returns:
            Dict with keys: id, slug, url, title; or None if not found
        """
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.row_factory = Row
            cursor.execute("SELECT id, slug, url, title FROM feeds WHERE slug = ?", (slug,))
            row = cursor.fetchone()
            if not row:
                return None
            return {
                'id': row['id'],
                'slug': row['slug'],
                'url': row['url'],
                'title': row['title'] or slug
            }
        except Error as e:
            logger.error(f"Error getting feed by slug {slug}: {e}")
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def query_latest_items_for_feed(self, slug: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Query the latest items for a specific feed slug (raw, unsummarized content passthrough).

        Args:
            slug: Feed slug
            limit: Maximum number of items

        Returns:
            List of items with keys: id, title, url, guid, date (unix), body
        """
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.row_factory = Row
            query = (
                """
                SELECT i.id, i.title, i.url, i.guid, i.date, i.body
                FROM items i
                JOIN feeds f ON f.id = i.feed_id
                WHERE f.slug = ?
                ORDER BY i.date DESC
                LIMIT ?
                """
            )
            cursor.execute(query, (slug, limit))
            rows = cursor.fetchall()
            items = []
            for row in rows:
                items.append({
                    'id': row['id'],
                    'title': row['title'],
                    'url': row['url'],
                    'guid': row['guid'],
                    'date': row['date'],
                    'body': row['body'],
                })
            return items
        except Error as e:
            logger.error(f"Error querying latest items for feed {slug}: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def reset_feed_error(self, feed_id: int) -> bool:
        """
        Reset the error count and last error for a feed after successful processing.
        
        Args:
            feed_id: The ID of the feed to reset errors for
            
        Returns:
            True if the reset was successful, False otherwise
        """
        cursor = None
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                UPDATE feeds 
                SET error_count = 0, last_error = NULL
                WHERE id = ?
            """, (feed_id,))
            
            success = cursor.rowcount > 0
            if success:
                self.conn.commit()
                logger.debug(f"Reset error status for feed {feed_id}")
            else:
                logger.warning(f"No feed found with ID {feed_id} to reset errors")
                
            return success
            
        except Error as e:
            logger.error(f"Error resetting feed error for feed {feed_id}: {e}")
            return False
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def create_bulletin(self, group_name: str, session_key: str, introduction: str, summary_ids: List[int], feed_slugs: List[str], title: Optional[str] = None) -> Optional[int]:
        """Create a new bulletin with the given parameters.
        
        Args:
            group_name: The summary group name (apple, business, etc.)
            session_key: The session identifier (YYYY-MM-DD-HH-MM or chunked)
            introduction: AI-generated introduction text
            summary_ids: List of summary IDs included in this bulletin
            feed_slugs: List of feed slugs used for this bulletin
        title: Optional AI-generated or fallback title
            
        Returns:
            The bulletin ID if successful, None otherwise
        """
        cursor = None
        try:
            
            with self.conn:
                cursor = self.conn.cursor()
                
                # Insert bulletin record
                cursor.execute("""
                    INSERT OR REPLACE INTO bulletins 
            (group_name, session_key, created_date, title, introduction, summary_count, feed_slugs)
            VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    group_name,
                    session_key, 
                    int(time()),
            title,
                    introduction,
                    len(summary_ids),
                    json.dumps(feed_slugs)
                ))
                
                bulletin_id = cursor.lastrowid
                
                # Insert bulletin-summary relationships
                if summary_ids:
                    # First, clear any existing relationships for this bulletin
                    cursor.execute("DELETE FROM bulletin_summaries WHERE bulletin_id = ?", (bulletin_id,))
                    
                    # Insert new relationships
                    relationships = [(bulletin_id, summary_id) for summary_id in summary_ids]
                    cursor.executemany("""
                        INSERT OR IGNORE INTO bulletin_summaries (bulletin_id, summary_id)
                        VALUES (?, ?)
                    """, relationships)
                
                logger.info(f"Created bulletin {bulletin_id} for group '{group_name}' session '{session_key}' with {len(summary_ids)} summaries")
                return bulletin_id
                
        except Error as e:
            logger.error(f"Error creating bulletin for group '{group_name}' session '{session_key}': {e}")
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def get_bulletin(self, group_name: str, session_key: str) -> Optional[Dict[str, Any]]:
        """Get a bulletin by group name and session key.
        
        Args:
            group_name: The summary group name
            session_key: The session identifier
            
        Returns:
            Dictionary with bulletin data including summaries, or None if not found
        """
        cursor = None
        try:
            
            cursor = self.conn.cursor()
            cursor.row_factory = Row
            
            # Get bulletin metadata
            cursor.execute("""
                SELECT id, group_name, session_key, created_date, title, introduction, 
                       summary_count, feed_slugs
                FROM bulletins
                WHERE group_name = ? AND session_key = ?
            """, (group_name, session_key))
            
            bulletin_row = cursor.fetchone()
            if not bulletin_row:
                return None
            
            # Get associated summaries
            cursor.execute("""
                SELECT 
                    s.id, s.summary_text, s.topic, s.generated_date, s.published_date,
                    i.title as item_title, i.url as item_url, i.date as item_date,
                    f.title as feed_title, f.slug as feed_slug
                FROM bulletin_summaries bs
                JOIN summaries s ON bs.summary_id = s.id
                JOIN items i ON s.id = i.id
                JOIN feeds f ON i.feed_id = f.id
                WHERE bs.bulletin_id = ?
                ORDER BY i.date DESC
            """, (bulletin_row['id'],))
            
            summary_rows = cursor.fetchall()
            
            # Convert summaries to list of dictionaries
            summaries = []
            for row in summary_rows:
                summaries.append({
                    'id': row['id'],
                    'summary_text': row['summary_text'],
                    'topic': row['topic'],
                    'generated_date': row['generated_date'],
                    'published_date': row['published_date'],
                    'item_title': row['item_title'],
                    'item_url': row['item_url'],
                    'item_date': row['item_date'],
                    'feed_title': row['feed_title'],
                    'feed_slug': row['feed_slug']
                })
            
            # Parse feed_slugs JSON
            feed_slugs = []
            try:
                if bulletin_row['feed_slugs']:
                    feed_slugs = json.loads(bulletin_row['feed_slugs'])
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"Could not parse feed_slugs for bulletin {bulletin_row['id']}")
            
            return {
                'id': bulletin_row['id'],
                'group_name': bulletin_row['group_name'],
                'session_key': bulletin_row['session_key'],
                'created_date': bulletin_row['created_date'],
                'title': bulletin_row['title'] if 'title' in bulletin_row.keys() else None,
                'introduction': bulletin_row['introduction'],
                'summary_count': bulletin_row['summary_count'],
                'feed_slugs': feed_slugs,
                'summaries': summaries
            }
            
        except Error as e:
            logger.error(f"Error getting bulletin for group '{group_name}' session '{session_key}': {e}")
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def get_bulletins_for_group(self, group_name: str, days_back: int = 7) -> List[Dict[str, Any]]:
        """Get all bulletins for a group within the specified time period.
        
        Args:
            group_name: The summary group name
            days_back: Number of days to look back
            
        Returns:
            List of bulletin dictionaries with metadata (no full summaries)
        """
        cursor = None
        try:
            
            cutoff_time = int(time()) - (days_back * 24 * 60 * 60)
            
            cursor = self.conn.cursor()
            cursor.row_factory = Row
            
            cursor.execute("""
                SELECT id, group_name, session_key, created_date, title, introduction, 
                       summary_count, feed_slugs
                FROM bulletins
                WHERE group_name = ? AND created_date >= ?
                ORDER BY created_date DESC
            """, (group_name, cutoff_time))
            
            bulletin_rows = cursor.fetchall()
            
            bulletins = []
            for row in bulletin_rows:
                # Parse feed_slugs JSON
                feed_slugs = []
                try:
                    if row['feed_slugs']:
                        feed_slugs = json.loads(row['feed_slugs'])
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Could not parse feed_slugs for bulletin {row['id']}")
                
                bulletins.append({
                    'id': row['id'],
                    'group_name': row['group_name'],
                    'session_key': row['session_key'],
                    'created_date': row['created_date'],
                    'title': row['title'] if 'title' in row.keys() else None,
                    'introduction': row['introduction'],
                    'summary_count': row['summary_count'],
                    'feed_slugs': feed_slugs
                })
            
            return bulletins
            
        except Error as e:
            logger.error(f"Error getting bulletins for group '{group_name}': {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def update_bulletin_title(self, group_name: str, session_key: str, title: str) -> bool:
        """Update the title for an existing bulletin."""
        cursor = None
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute(
                    "UPDATE bulletins SET title = ? WHERE group_name = ? AND session_key = ?",
                    (title, group_name, session_key)
                )
                return cursor.rowcount > 0
        except Error as e:
            logger.error(f"Error updating bulletin title for {group_name}/{session_key}: {e}")
            return False
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass

    def delete_old_bulletins(self, days_to_keep: int = 7) -> int:
        """Delete bulletins older than the specified number of days.
        
        Args:
            days_to_keep: Number of days of bulletins to keep
            
        Returns:
            Number of bulletins deleted
        """
        cursor = None
        try:
            
            cutoff_time = int(time()) - (days_to_keep * 24 * 60 * 60)
            
            with self.conn:
                cursor = self.conn.cursor()
                
                # Delete old bulletins (cascade will handle bulletin_summaries)
                cursor.execute("DELETE FROM bulletins WHERE created_date < ?", (cutoff_time,))
                
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(f"Deleted {deleted_count} old bulletins (older than {days_to_keep} days)")
                
                return deleted_count
                
        except Error as e:
            logger.error(f"Error deleting old bulletins: {e}")
            return 0
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
