import pytest
from datetime import datetime, timezone, timedelta
from config import config
from models import DatabaseQueue
from summarizer import NewsProcessor
from aiohttp import ClientSession

@pytest.mark.asyncio
async def test_summarizer_respects_time_window(tmp_path, monkeypatch):
    # Prepare temporary database
    db_path = tmp_path / "test.db"
    original_db = config.DATABASE_PATH
    config.DATABASE_PATH = str(db_path)

    processor = NewsProcessor()
    await processor.initialize()

    try:
        # Register feed
        assert await processor.db.execute('register_feed', slug='testfeed', url='http://example.com')
        feed_id = await processor.db.execute('get_feed_id', slug='testfeed')
        assert feed_id is not None

        now_ts = int(datetime.now(timezone.utc).timestamp())
        old_ts = now_ts - 72 * 3600  # 72h old (outside 48h window)

        cur = processor.db.conn.cursor()
        cur.execute("INSERT INTO items (feed_id, title, url, guid, body, date) VALUES (?, ?, ?, ?, ?, ?)", (feed_id, 'Fresh Item', 'http://example.com/fresh', 'guid-fresh', 'Body fresh', now_ts))
        cur.execute("INSERT INTO items (feed_id, title, url, guid, body, date) VALUES (?, ?, ?, ?, ?, ?)", (feed_id, 'Old Item', 'http://example.com/old', 'guid-old', 'Body old', old_ts))
        processor.db.conn.commit()
        cur.close()

        # Capture IDs
        cur = processor.db.conn.cursor()
        cur.execute("SELECT id, title FROM items ORDER BY id")
        rows = cur.fetchall()
        cur.close()
        ids_by_title = {row[1]: row[0] for row in rows}
        fresh_id = ids_by_title['Fresh Item']
        old_id = ids_by_title['Old Item']

        # Monkeypatch call_azure_openai to return both IDs (including one outside window)
        async def fake_call(prompt_text, session):
            # Return a well-formed JSON array with both IDs; old_id should be ignored downstream
            return (
                f"[{{\n  \"id\": {fresh_id}, \"topic\": \"Tech\", \"summary\": \"Fresh summary\"\n}},"
                f"{{\n  \"id\": {old_id}, \"topic\": \"Tech\", \"summary\": \"Old summary\"\n}}]"
            )
        monkeypatch.setattr(processor, 'call_azure_openai', fake_call)

        # Run process_feed
        async with ClientSession() as http_session:
            summarized_count = await processor.process_feed('testfeed', http_session)

        # Only the fresh item should be summarized
        assert summarized_count == 1

        # Check summaries table
        cur = processor.db.conn.cursor()
        cur.execute("SELECT id FROM summaries")
        summarized_ids = {row[0] for row in cur.fetchall()}
        cur.close()
        assert fresh_id in summarized_ids
        assert old_id not in summarized_ids
    finally:
        await processor.close()
        config.DATABASE_PATH = original_db
