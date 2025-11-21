import pytest
from llm_client import chat_completion


class FakeChoice:
    def __init__(self):
        class Msg:
            # Simulate list-of-parts with non-standard type plus empty text
            content = [
                {"type": "reasoning", "text": ""},
                {"type": "metadata", "text": ""},
            ]
        self.message = Msg()
        self.finish_reason = "length"


class FakeResp:
    def __init__(self):
        self.choices = [FakeChoice()]


class FakeClient:
    class chat:
        class completions:
            @staticmethod
            async def create(**kwargs):  # type: ignore
                return FakeResp()


@pytest.mark.asyncio
async def test_truncated_empty_content_placeholder(monkeypatch):
    result = await chat_completion(
        messages=[{"role": "user", "content": "Test truncated scenario"}],
        purpose="summaries",
        retries=0,
        client_override=FakeClient(),
    )
    assert result == "[Truncated output: no content returned]"