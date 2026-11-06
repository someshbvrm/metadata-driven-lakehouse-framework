import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from lakehouse_framework.spark import get_spark

    session = get_spark("lakehouse-framework-tests")
    yield session
    session.stop()
