import pytest


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """Anyio backend.

    :return: backend name.
    """
    return "asyncio"
