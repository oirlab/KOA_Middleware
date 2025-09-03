import pytest
import datetime
import uuid

from koa_middleware.store import CalibrationStore
from .calibration_orm_model import CalibrationTestORM

def test_register_local_calibration(in_memory_calibration_store):
    """
    Tests the register_local_calibration method.
    """
    store = in_memory_calibration_store

    # Create a mock CalibrationTestORM instance
    test_calibration_id = str(uuid.uuid4())
    test_filename = "test_registered_calibration.fits"
    test_mjd_start = 60100.0
    test_datetime_obs = datetime.datetime.now().isoformat()

    mock_cal = CalibrationTestORM(
        id=test_calibration_id,
        filename=test_filename,
        master=True,
        cal_type="flat",
        instera="test_era_2",
        spectrograph="spectrograph_b",
        mjd_start=test_mjd_start,
        last_updated=test_datetime_obs
    )

    # Register the calibration
    registered_cal = store.register_local_calibration(mock_cal)

    # Verify that the calibration was added to the local database
    retrieved_cal = store.local_db.query_by_id(test_calibration_id)
    assert retrieved_cal is not None
    assert len(retrieved_cal) == 1
    assert retrieved_cal[0].id == test_calibration_id
    assert retrieved_cal[0].filename == test_filename
