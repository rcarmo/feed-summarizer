import asyncio
import os
from datetime import datetime, timezone, timedelta
import pytest

from models import DatabaseQueue
from config import config

@pytest.mark.asyncio
async def test_time_window_filter_and_retention(tmp_path):
    # Setup temporary database
    db_path = tmp_path / "test.db"
    db = DatabaseQueue(str(db_path))
    await db.start()

    try:
        # Register feed
        assert await db.execute('register_feed', slug='testfeed', url='http://example.com')
        feed_id = await db.execute('get_feed_id', slug='testfeed')
        assert feed_id is not None

        now = int(datetime.now(timezone.utc).timestamp())
        one_day = now - 24*3600
        three_days = now - 72*3600

        cursor = db.conn.cursor()
        # Insert items (id auto increments via rowid substitution using feed-specific uniqueness)
        # We'll explicitly set ids via insertion ordering; guid uniqueness avoids duplicates.
        cursor.execute("INSERT INTO items (feed_id, title, url, guid, body, date) VALUES (?, ?, ?, ?, ?, ?)", (feed_id, 'Now Item', 'http://example.com/now', 'guid-now', 'Body now', now))
        cursor.execute("INSERT INTO items (feed_id, title, url, guid, body, date) VALUES (?, ?, ?, ?, ?, ?)", (feed_id, 'One Day Item', 'http://example.com/1d', 'guid-1d', 'Body 1d', one_day))
        cursor.execute("INSERT INTO items (feed_id, title, url, guid, body, date) VALUES (?, ?, ?, ?, ?, ?)", (feed_id, 'Three Day Item', 'http://example.com/3d', 'guid-3d', 'Body 3d', three_days))
        db.conn.commit()
        cursor.close()

        # Query with 48h window should exclude 72h old item
        items_recent = await db.execute('query_raw_feeds', slugs=['testfeed'], cutoff_age_hours=48)
        titles = {it['title'] for it in items_recent}
        assert 'Now Item' in titles
        assert 'One Day Item' in titles
        assert 'Three Day Item' not in titles

        # Force retention_days small for test purge
        original_retention = getattr(config, 'RETENTION_DAYS', 7)
        config.RETENTION_DAYS = 2
        deleted = await db.execute('expire_old_entries', expiration_days=config.RETENTION_DAYS)
        assert deleted >= 1  # At least the three-day item should be deleted

        # Verify remaining items
        cursor = db.conn.cursor()
        cursor.execute("SELECT title FROM items")
        remaining = {row[0] for row in cursor.fetchall()}
        cursor.close()
        assert 'Three Day Item' not in remaining
        assert 'Now Item' in remaining and 'One Day Item' in remaining
    finally:
        # Restore retention
        config.RETENTION_DAYS = getattr(config, 'RETENTION_DAYS', 7)
        await db.stop()
