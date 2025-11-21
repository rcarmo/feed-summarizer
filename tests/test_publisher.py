import pytest
from datetime import datetime, timezone
from publisher import RSSPublisher

@pytest.fixture
def sample_feeds_info():
    return [
        {
            'name': 'tech',
            'title': 'Tech News',
            'description': 'Latest updates in technology.',
            'filename': 'tech.xml',
            'latest_title': 'AI Revolution'
        },
        {
            'name': 'business',
            'title': 'Business Insights',
            'description': 'Market trends and analysis.',
            'filename': 'business.xml',
            'latest_title': None
        }
    ]

@pytest.fixture
def sample_passthrough_info():
    return [
        {
            'name': 'raw_feed',
            'title': 'Raw Feed',
            'filename': 'raw_feed.xml'
        }
    ]

import asyncio

@pytest.mark.asyncio
async def test_write_index_html(tmp_path):
    publisher = RSSPublisher()
    publisher.rss_feeds_dir = tmp_path / "feeds"
    publisher.rss_feeds_dir.mkdir()
    # Create dummy RSS files
    (publisher.rss_feeds_dir / "tech.xml").write_text("<rss></rss>")
    (publisher.rss_feeds_dir / "business.xml").write_text("<rss></rss>")
    await publisher._write_index_html()
    index_path = publisher.rss_feeds_dir / "index.html"
    assert index_path.exists()
    html = index_path.read_text()
    assert '<h1>' in html
    assert 'Tech' in html or 'Tech News' in html
    assert 'Business' in html or 'Business Insights' in html