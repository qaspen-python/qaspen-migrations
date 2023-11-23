import pytest

pytestmark = [pytest.mark.anyio]


async def test_default() -> None:
    assert str(1) == "1"
