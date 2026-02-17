import pytest
import datetime
import uuid
from pathlib import Path

from koa_middleware.store import CalibrationStore
from .test_selectors import _TestDarkSelector

@pytest.fixture
def in_memory_calibration_store():
    """
    Fixture for an in-memory CalibrationStore using sqlite-utils.
    """
    cache_dir = Path("/tmp/koa_middleware_test_cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Initialize CalibrationStore with in-memory SQLite
    store = CalibrationStore(
        instrument_name="test_instrument",
        cache_dir=str(cache_dir),
        local_database_filename=":memory:",
        connect_remote=False  # Only local DB
    )

    yield store

    # Clean up
    store.close()
    import shutil
    shutil.rmtree(cache_dir)


def test_minimal_koa_middleware_integration(in_memory_calibration_store):
    """
    Tests minimal integration of KOA Middleware with an in-memory SQLite database using sqlite-utils.
    """
    store = in_memory_calibration_store

    # 1. Define a minimal calibration entry as a dict
    test_calibration_id = str(uuid.uuid4())
    test_filename = "test_dark_calibration.fits"
    test_mjd_start = 60000.0  # Example MJD
    test_datetime_obs = datetime.datetime.now().isoformat()

    minimal_cal = {
        "id": test_calibration_id,
        "filename": test_filename,
        "master_cal": 1,  # sqlite-utils stores booleans as int
        "cal_type": "dark",
        "instrument_era": "test_era_1",
        "spectrograph": "spectrograph_a",
        "mjd_start": test_mjd_start,
        "datetime_obs": test_datetime_obs,
    }

    # 2. Add the minimal calibration entry to the in-memory database
    store.local_db.add(minimal_cal)

    # 3. Use a DarkSelector to retrieve the calibration
    dark_selector = _TestDarkSelector()
    input_meta = {
        'instrument_name': 'test_instrument',
        'instrument_era': 'test_era_1',
        'spectrograph': 'spectrograph_a',
        'mjd_start': test_mjd_start + 0.001  # Slightly different MJD to test proximity
    }

    selected_entry = dark_selector.select(input_meta, store.local_db)

    # 4. Verify the retrieved calibration
    assert selected_entry is not None
    assert selected_entry["id"] == test_calibration_id
    assert selected_entry["filename"] == test_filename
    assert selected_entry["cal_type"] == "dark"
    assert selected_entry["instrument_era"] == "test_era_1"
    assert selected_entry["spectrograph"] == "spectrograph_a"