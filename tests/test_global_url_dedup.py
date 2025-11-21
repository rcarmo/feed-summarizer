import asyncio
import pytest
import feedparser
from fetcher import FeedFetcher
from config import config


@pytest.mark.asyncio
async def test_global_url_dedup(monkeypatch, tmp_path):
    """Ensure that identical article URLs across different feeds are not reprocessed.

    Scenario:
        Two feeds (reg_a, reg_b) emit overlapping articles with the same URL but different GUIDs.
        We expect only one stored item for the shared URL and the second feed to treat it as existing
        (i.e., not insert a duplicate row).
    """
    # Use isolated temp database
    test_db_path = tmp_path / "test.db"
    monkeypatch.setattr(config, 'DATABASE_PATH', str(test_db_path))

    fetcher = FeedFetcher()
    await fetcher.initialize()

    # Register two feeds representing category variants
    await fetcher.db.execute('register_feed', slug='reg_a', url='https://example.com/a.atom')
    await fetcher.db.execute('register_feed', slug='reg_b', url='https://example.com/b.atom')
    feed_a_id = await fetcher.db.execute('get_feed_id', slug='reg_a')
    feed_b_id = await fetcher.db.execute('get_feed_id', slug='reg_b')

    # Build mock entries (feedparser-like objects) for first feed
    # Two distinct articles
    entry1 = feedparser.FeedParserDict({
        'link': 'https://theregister.co.uk/article/12345',
        'title': 'Article One',
        'id': 'GUID-A-1',
        'summary': '<p>Content 1</p>'
    })
    entry2 = feedparser.FeedParserDict({
        'link': 'https://theregister.co.uk/article/67890',
        'title': 'Article Two',
        'id': 'GUID-A-2',
        'summary': '<p>Content 2</p>'
    })
    entries_a = [entry1, entry2]

    # Process first feed
    await fetcher._process_feed_entries(feed_a_id, 'reg_a', entries_a, post_process=False, reader_mode=False, session=None)  # type: ignore

    count_after_a = await fetcher.db.execute('count_items')
    assert count_after_a == 2, f"Expected 2 items after first feed, got {count_after_a}"

    # Second feed emits one overlapping (entry1 URL) and one new article
    entry3_dup = feedparser.FeedParserDict({
        'link': 'https://theregister.co.uk/article/12345',
        'title': 'Article One (Duplicate)',
        'id': 'GUID-B-1',
        'summary': '<p>Dup Content</p>'
    })
    entry4_new = feedparser.FeedParserDict({
        'link': 'https://theregister.co.uk/article/99999',
        'title': 'Article Three',
        'id': 'GUID-B-2',
        'summary': '<p>Content 3</p>'
    })
    entries_b = [entry3_dup, entry4_new]

    await fetcher._process_feed_entries(feed_b_id, 'reg_b', entries_b, post_process=False, reader_mode=False, session=None)  # type: ignore

    count_after_b = await fetcher.db.execute('count_items')
    # Should only increase by 1 (the truly new URL), not by 2
    assert count_after_b == 3, f"Expected 3 total items after second feed, got {count_after_b} (duplicate URL should be skipped)"

    await fetcher.close()