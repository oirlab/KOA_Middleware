import pytest
import datetime
import uuid

from koa_middleware.store import CalibrationStore
from .calibration_orm_model import CalibrationTestORM
from .test_selectors import TestDarkSelector

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
        master=True,
        cal_type="dark",
        instera="test_era_1",
        spectrograph="spectrograph_a",
        mjd_start=test_mjd_start,
        last_updated=test_datetime_obs
    )

    # 2. Add the minimal HISPECCalibrationORM instance to the in-memory database
    store.local_db.add(minimal_cal)

    # 3. Use a DarkSelector to retrieve the calibration
    dark_selector = TestDarkSelector()
    input_meta = {
        'instrument.name': 'test_instrument',
        'instrument.era': 'test_era_1',
        'instrument.spectrograph': 'spectrograph_a',
        'exposure.mjd_start': test_mjd_start + 0.001 # Slightly different MJD to test proximity
    }

    selected_orm_instance = dark_selector.select(input_meta, store.local_db)

    # 4. Verify the retrieved calibration
    assert selected_orm_instance is not None
    assert selected_orm_instance.id == test_calibration_id
    assert selected_orm_instance.filename == test_filename
    assert selected_orm_instance.cal_type == "dark"
    assert selected_orm_instance.instera == "test_era_1"
    assert selected_orm_instance.spectrograph == "spectrograph_a"
