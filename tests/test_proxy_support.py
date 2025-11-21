from config import config
from fetcher import FeedFetcher


def test_proxy_true_uses_global_proxy(monkeypatch):
    fetcher = FeedFetcher()
    monkeypatch.setattr(config, "PROXY_URL", "http://proxy.example:3128")

    proxy_url = fetcher._resolve_proxy_url("example", {"proxy": True})

    assert proxy_url == "http://proxy.example:3128"


def test_proxy_string_overrides_global(monkeypatch):
    fetcher = FeedFetcher()
    monkeypatch.setattr(config, "PROXY_URL", "http://global:8080")

    proxy_url = fetcher._resolve_proxy_url("example", {"proxy": "http://custom:9000"})

    assert proxy_url == "http://custom:9000"


def test_proxy_true_without_global_returns_none(monkeypatch):
    fetcher = FeedFetcher()
    monkeypatch.setattr(config, "PROXY_URL", None)

    proxy_url = fetcher._resolve_proxy_url("missing-global", {"proxy": True})

    assert proxy_url is None
