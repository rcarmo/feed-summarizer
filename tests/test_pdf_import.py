import pytest

def test_pypdf_import():
    try:
        from pypdf import PdfReader  # noqa: F401
    except Exception as e:
        pytest.fail(f"Failed to import pypdf PdfReader: {e}")
