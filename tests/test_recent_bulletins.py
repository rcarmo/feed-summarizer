from pathlib import Path
from publisher import RSSPublisher
from config import config
import asyncio


def test_build_recent_bulletins(tmp_path: Path, monkeypatch):
    # Arrange: create fake public/bulletins dir under tmp_path and monkeypatch config.PUBLIC_DIR
    bulletins_dir = tmp_path / 'public' / 'bulletins'
    bulletins_dir.mkdir(parents=True)

    sample_html = '''<!DOCTYPE html><html><body>
    <div class="header"><h1>AI Bulletin</h1></div>
    <div class="introduction"><h2>Overview</h2><p>This is an overview of recent developments in AI safety and tooling.</p></div>
    <div class="topic-section"><div class="summary-item"><div class="summary-text">First summary paragraph about AI models.</div></div></div>
    </body></html>'''
    (bulletins_dir / 'ai.html').write_text(sample_html, encoding='utf-8')

    class DummyConfig:
        PUBLIC_DIR = str(tmp_path / 'public')
        RSS_BASE_URL = 'https://example.test'

    # Monkeypatch config attributes used in RSSPublisher.__init__
    monkeypatch.setattr(config, 'PUBLIC_DIR', DummyConfig.PUBLIC_DIR, raising=False)
    monkeypatch.setattr(config, 'DATA_PATH', tmp_path, raising=False)

    # Act
    publisher = RSSPublisher(base_url='https://example.test', enable_azure_upload=False)
    latest_titles = {'ai': 'AI Bulletin Title'}
    recent = publisher.build_recent_bulletins(latest_titles)

    # Assert
    assert len(recent) == 1
    assert recent[0]['filename'] == 'ai.html'
    assert recent[0]['title'] == 'AI Bulletin Title'
    assert 'overview of recent developments' in recent[0]['summary'].lower()
    assert len(recent[0]['summary']) <= 140
