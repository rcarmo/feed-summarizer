from utils import clean_html_to_markdown


def test_clean_html_resolves_relative_urls_with_base():
    html = '<p><a href="details.html">Read more</a><img src="../img/photo.png" alt="Photo"/></p>'
    markdown = clean_html_to_markdown(html, base_url="https://example.com/articles/2025/")

    assert "[Read more](https://example.com/articles/2025/details.html)" in markdown
    assert "https://example.com/articles/img/photo.png" in markdown


def test_clean_html_relative_urls_without_base_neutralized():
    html = '<p><a href="details.html">Read more</a><img src="img/photo.png" alt="Photo"/></p>'
    markdown = clean_html_to_markdown(html)

    assert "[Read more](#)" in markdown
    assert "img/photo.png" not in markdown
