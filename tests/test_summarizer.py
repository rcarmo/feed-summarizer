import pytest
import asyncio
from summarizer import NewsProcessor

@pytest.fixture
def sample_entries():
    return [
        {
            'topic': 'AI',
            'title': 'AI in 2025',
            'summary': 'The state of AI in 2025.',
            'link': 'http://example.com/ai-2025'
        },
        {
            'topic': 'AI',
            'title': 'AI Ethics',
            'summary': 'Ethical considerations in AI.',
            'link': 'http://example.com/ai-ethics'
        },
        {
            'topic': 'Tech',
            'title': 'Quantum Computing',
            'summary': 'Advances in quantum computing.',
            'link': 'http://example.com/quantum-computing'
        }
    ]

@pytest.mark.asyncio
async def test_group_by_topic_and_generate_markdown(sample_entries):
    processor = NewsProcessor()
    # Fake url_ids mapping for test
    url_ids = {1: 'http://example.com/ai-2025', 2: 'http://example.com/ai-ethics', 3: 'http://example.com/quantum-computing'}
    # Build minimal JSON content for the method
    json_content = '[{"topic": "AI", "id": 1, "summary": "The state of AI in 2025."}, {"topic": "AI", "id": 2, "summary": "Ethical considerations in AI."}, {"topic": "Tech", "id": 3, "summary": "Advances in quantum computing."}]'
    markdown, seen_ids, summaries_dict = await processor.group_by_topic_and_generate_markdown(json_content, url_ids)
    assert '# AI' in markdown
    assert 'Ethical considerations in AI.' in markdown
    assert '# Tech' in markdown
    assert 'Advances in quantum computing.' in markdown
    assert all(len(entry) == 3 for entry in summaries_dict.values())
    assert any(entry[2] for entry in summaries_dict.values())