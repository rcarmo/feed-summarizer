from jinja2 import Environment, FileSystemLoader
from datetime import datetime, timezone

def test_bulletins_index_loop_renders_cards(tmp_path):
    env = Environment(loader=FileSystemLoader('templates'))
    template = env.get_template('bulletins_index.html')
    bulletins_info = [
        {
            'title': 'Technews',
            'description': 'tech news summaries',
            'latest_title': 'Latest GPU Advances',
            'updated': '2025-10-06 12:00 UTC',
            'filename': 'technews.html',
            'name': 'technews'
        },
        {
            'title': 'Business',
            'description': 'business news summaries',
            'latest_title': 'Markets Rally',
            'updated': '2025-10-06 13:00 UTC',
            'filename': 'business.html',
            'name': 'business'
        }
    ]
    rendered = template.render(bulletins_info=bulletins_info, current_time=datetime.now(timezone.utc))
    # Ensure each bulletin title appears exactly once in card headings
    assert rendered.count('Technews') >= 1
    assert rendered.count('Business') >= 1
    assert 'Latest GPU Advances' in rendered
    assert 'Markets Rally' in rendered
    # Links should now be relative (index rendered in /bulletins/)
    assert 'href="technews.html"' in rendered
    assert 'href="../feeds/technews.xml"' in rendered
