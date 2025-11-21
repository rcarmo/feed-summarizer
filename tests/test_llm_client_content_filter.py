import pytest
from llm_client import chat_completion
from summarizer import ContentFilterError


class FakeChoice:
    def __init__(self, message):
        self.message = message
        self.finish_reason = "stop"


class FakeResp:
    def __init__(self, choices):
        self.choices = choices


class FakeClient:
    class chat:
        class completions:
            @staticmethod
            async def create(**kwargs):  # type: ignore
                # Simulate content filter error by raising OpenAIError-like object
                class DummyOpenAIError(Exception):
                    def __init__(self):
                        self.body = {
                            "error": {
                                "code": "content_filter",
                                "message": "Content filtered by Azure OpenAI",
                                "innererror": {"code": "ResponsibleAIPolicyViolation"},
                                "param": None,
                            }
                        }
                raise DummyOpenAIError()


@pytest.mark.asyncio
async def test_llm_client_content_filter(monkeypatch):
    with pytest.raises(ContentFilterError) as excinfo:
        await chat_completion(
            messages=[{"role": "user", "content": "test"}],
            purpose="test",
            retries=0,
            client_override=FakeClient(),
        )
    assert "Content filtered" in str(excinfo.value)
    assert excinfo.value.details.get("code") == "content_filter"
    assert excinfo.value.details.get("innererror", {}).get("code") == "ResponsibleAIPolicyViolation"
