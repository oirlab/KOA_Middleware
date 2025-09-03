import pytest
import uuid
import datetime
from pathlib import Path

from koa_middleware.store import CalibrationStore
from .calibration_orm_model import CalibrationTestORM

@pytest.fixture
def in_memory_calibration_store():
    """
    Fixture for an in-memory CalibrationStore.
    """
    cache_dir = Path("/tmp/koa_middleware_test_cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    store = CalibrationStore(
        orm_class=CalibrationTestORM,
        cache_dir=str(cache_dir),
        local_database_filename=":memory:",
        remote_database_url=None,
        calibrations_url=None
    )
    yield store
    store.close()
    import shutil
    shutil.rmtree(cache_dir)
