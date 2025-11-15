import os
import pytest
import datetime
import uuid
from pathlib import Path

from koa_middleware.store import CalibrationStore
from .calibration_orm_model import CalibrationTestORM
from .test_selectors import TestDarkSelector

@pytest.fixture
def in_memory_calibration_store():
    """
    Fixture for an in-memory CalibrationStore.
    """
    # Create a dummy cache directory for the store, though it won't be used for in-memory DB
    cache_dir = Path("/tmp/koa_middleware_test_cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Initialize CalibrationStore with in-memory SQLite
    store = CalibrationStore(
        orm_class=CalibrationTestORM,
        cache_dir=str(cache_dir),
        local_database_filename=":memory:",
        remote_database_url=None, # Ensure no remote connection
        calibrations_url=None # Ensure no remote connection
    )
    yield store
    # Clean up the dummy cache directory after the test
    store.close()
    import shutil
    shutil.rmtree(cache_dir)

def test_minimal_koa_middleware_integration(in_memory_calibration_store):
    """
    Tests minimal integration of KOA Middleware with an in-memory SQLite database.
    """
    store = in_memory_calibration_store

    # 1. Define a minimal HISPECCalibrationORM instance
    test_calibration_id = str(uuid.uuid4())
    test_filename = "test_dark_calibration.fits"
    test_mjd_start = 60000.0 # Example MJD
    test_datetime_obs = datetime.datetime.now().isoformat()

    minimal_cal = CalibrationTestORM(
        id=test_calibration_id,
        filename=test_filename,
        master_cal=True,
        cal_type="dark",
        instrument_era="test_era_1",
        spectrograph="spectrograph_a",
        mjd_start=test_mjd_start,
        last_updated=test_datetime_obs
    )

    # 2. Add the minimal HISPECCalibrationORM instance to the in-memory database
    store.local_db.add(minimal_cal)

    # 3. Use a DarkSelector to retrieve the calibration
    dark_selector = TestDarkSelector()
    input_meta = {
        'instrument_name': 'test_instrument',
        'instrument_era': 'test_era_1',
        'spectrograph': 'spectrograph_a',
        'mjd_start': test_mjd_start + 0.001 # Slightly different MJD to test proximity
    }

    selected_orm_instance = dark_selector.select(input_meta, store.local_db)

    # 4. Verify the retrieved calibration
    assert selected_orm_instance is not None
    assert selected_orm_instance.id == test_calibration_id
    assert selected_orm_instance.filename == test_filename
    assert selected_orm_instance.cal_type == "dark"
    assert selected_orm_instance.instrument_era == "test_era_1"
    assert selected_orm_instance.spectrograph == "spectrograph_a"
