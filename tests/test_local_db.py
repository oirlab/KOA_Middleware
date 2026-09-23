import os
from sys import version
import uuid
from datetime import datetime, timezone, timedelta

from koa_middleware.store import CalibrationStore
from koa_middleware.utils import isot_to_mjd, mjd_to_isot_ms, datetime_to_isot_ms


class CalModel:
    """
    Lightweight data model implements
    SupportsCalibrationIO protocol save() and to_record().
    """
    def __init__(
        self,
        cal_type: str,
        datetime_obs: str,
    ):
        self.meta = {
            "id": str(uuid.uuid4()),
            "filename": f"cal_{cal_type}_{datetime_obs.replace(':', '').replace('-', '').replace('T', '_').replace('.', '')}.fits",
            "cal_type": cal_type,
            "datetime_obs": datetime_obs,
        }

    def save(self, output_dir: str | None = None, output_path: str | None = None) -> str:
        if output_path is None:
            if output_dir is None:
                output_dir = "."
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, self.meta["filename"])
        # Write an empty file to simulate a calibration FITS
        with open(output_path, "wb") as f:
            f.write(b"")
        return output_path

    def to_record(self) -> dict:
        return self.meta
    

class MyCalibrationStore(CalibrationStore):
    """
    Subclass of CalibrationStore that overrides remote DB connection to avoid external calls during testing.
    """
    def _finalize_cal_model(
        self,
        cal,
        cal_version: str | None = None,
        origin: str | None = None
    ):
        cal.meta['cal_version'] = cal_version
        cal.meta['origin'] = origin


def test_local_db_basic(tmp_path):
    cache_dir = str(tmp_path)

    with MyCalibrationStore(
        instrument_name="test_instrument",
        cache_dir=cache_dir,
        local_database_filename=":memory:",
        connect_remote=False
    ) as store:

        # Ensure clean DB state
        store.local_db._reset(confirm=True)

        dt_before_register = datetime_to_isot_ms(datetime.now(timezone.utc))

        # Register 5 local calibrations
        base_dt = datetime_to_isot_ms(datetime.now(timezone.utc))
        base_mjd = isot_to_mjd(base_dt)
        N = 5
        for i in range(N):
            model = CalModel(
                cal_type="dark",
                datetime_obs=mjd_to_isot_ms(base_mjd + i),
            )
            local_path, _ = store.register_calibration(model, origin='LOCAL')
            assert os.path.isfile(local_path), f"Error: Calibration file not saved: {local_path}"

        dt_after_register = datetime_to_isot_ms(datetime.now(timezone.utc))

        # Query all local entries
        all_rows = store.local_db.query()
        assert isinstance(all_rows, list)
        assert len(all_rows) == 5

        # Query by ID
        first_id = all_rows[0]["id"]
        row_by_id = store.local_db.query_id(first_id)
        assert row_by_id is not None and row_by_id["id"] == first_id

        # Verify get_last_updated
        last_updated = store.local_db.get_last_updated()
        assert isinstance(last_updated, str)
        assert dt_before_register <= last_updated <= dt_after_register

        # Query by last_updated range (start only)
        lu_start = datetime_to_isot_ms(datetime.fromisoformat(dt_before_register))
        rows_lu = store.local_db.query(last_updated_start=lu_start)
        assert all(r["last_updated"] >= lu_start for r in rows_lu)

        # Query by datetime range
        dt_end = datetime_to_isot_ms(datetime.fromisoformat(base_dt) + timedelta(days=2))
        rows_dt = store.local_db.query(date_time_start=base_dt, date_time_end=dt_end)
        assert len(rows_dt) == 3
        assert all(base_dt <= r["datetime_obs"] <= dt_end for r in rows_dt)

        # Ensure total count is still 5 after queries
        assert len(store.local_db) == 5


def test_sync_local_db_from_cached_files(tmp_path):
    cache_dir = str(tmp_path)

    # Create dummy calibration files and metadata
    models = []
    base_dt = datetime_to_isot_ms(datetime.now(timezone.utc))
    N = 3
    for i in range(N):
        dt_obs = datetime_to_isot_ms(datetime.fromisoformat(base_dt) + timedelta(days=i))
        model = CalModel(
            cal_type="flat",
            datetime_obs=dt_obs,
        )
        model.save(output_dir=cache_dir)
        models.append(model)

    with MyCalibrationStore(
        instrument_name="test_instrument",
        cache_dir=cache_dir,
        local_database_filename=":memory:",
        connect_remote=False
    ) as store:

        # Ensure clean DB state
        store.local_db._reset(confirm=True)

        # Populate local DB from existing cache
        store.sync_records_from_cached_files(models)

        # Verify entries in local DB
        all_rows = store.local_db.query()
        assert len(all_rows) == N
        for model in models:
            matching = [r for r in all_rows if r["id"] == model.meta["id"]]
            assert len(matching) == 1

        # Ensure clean DB state
        store.local_db._reset(confirm=True)

        # Populate local DB from existing cache
        store.sync_records_from_cached_files(models)

        # Verify entries in local DB
        all_rows = store.local_db.query()
        assert len(all_rows) == N
        for model in models:
            matching = [r for r in all_rows if r["id"] == model.meta["id"]]
            assert len(matching) == 1

def test_calibration_versioning(tmp_path):
    
    N = 3
    dark_models = []

    cache_dir = str(tmp_path)

    # Create N dark models
    #t = astropy.time.Time.now()
    base_dt = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    base_mjd = isot_to_mjd(base_dt)
    for i in range(N):
        model = CalModel(
            cal_type="dark",
            datetime_obs=base_dt,
        )
        dark_models.append(model)

    # Initialize store
    with MyCalibrationStore(
        instrument_name="test_instrument",
        cache_dir=cache_dir,
        local_database_filename=":memory:",
        connect_remote=False
    ) as store:

        # Register each dark and check version increases since same dark
        for idx, dark_model in enumerate(dark_models):
            version = store._get_next_calibration_version(dark_model, origin='LOCAL')
            print(f"Dark model {idx}: version {version}")
            expected_version = f"{idx+1:03d}"  # "001", "002", ...
            assert version == expected_version, f"Expected {expected_version}, got {version}"

            # Register to DB to increment versions
            local_filepath, _ = store.register_calibration(dark_model, new_version=True, origin='LOCAL')

            assert os.path.isfile(local_filepath), f"Error: Calibration file not saved: {local_filepath}"

        # Change datetime_obs and ensure version starts over at "001"
        new_dark_model = CalModel(
            cal_type="dark",
            datetime_obs=mjd_to_isot_ms(base_mjd + 1)
        )
        next_version = store._get_next_calibration_version(new_dark_model, origin='LOCAL')
        assert next_version == "001", f"Expected version to reset to '001' for new family, but got {next_version}"

        # Change cal_type and filename and ensure version starts over at "001"
        new_flat_model = CalModel(
            cal_type="flat",
            datetime_obs=mjd_to_isot_ms(base_mjd + 2)
        )
        next_version = store._get_next_calibration_version(new_flat_model, origin='LOCAL')
        assert next_version == "001", f"Expected version to reset to '001' for new family, but got {next_version}"


def test_register_calibration_override_latest(tmp_path):
    cache_dir = str(tmp_path)

    dt_obs = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

    with MyCalibrationStore(
        instrument_name="test_instrument",
        cache_dir=cache_dir,
        local_database_filename=":memory:",
        connect_remote=False
    ) as store:

        # Ensure clean DB state
        store.local_db._reset(confirm=True)

        # Register the initial version
        first_model = CalModel(cal_type="dark", datetime_obs=dt_obs)
        first_filepath, first_record = store.register_calibration(first_model, origin='LOCAL')
        assert os.path.isfile(first_filepath)
        assert first_record["cal_version"] == "001"
        assert len(store.local_db) == 1

        # Registering a normal calibration in the same version family without new_version
        # should be skipped (no-op), since one already exists.
        skip_model = CalModel(cal_type="dark", datetime_obs=dt_obs)
        skip_filepath, skip_record = store.register_calibration(skip_model, origin='LOCAL')
        assert skip_filepath is None and skip_record is None
        assert len(store.local_db) == 1

        # new_version and override_latest are mutually exclusive
        try:
            store.register_calibration(skip_model, origin='LOCAL', new_version=True, override_latest=True)
            assert False, "Expected ValueError for mutually exclusive arguments"
        except ValueError:
            pass

        # Override the latest version with new data instead of bumping the version
        override_model = CalModel(cal_type="dark", datetime_obs=dt_obs)
        original_override_id = override_model.meta["id"]
        override_filepath, override_record = store.register_calibration(
            override_model, origin='LOCAL', override_latest=True
        )
        assert os.path.isfile(override_filepath)
        # Still only a single entry in the version family, version unchanged
        assert len(store.local_db) == 1
        assert override_record["cal_version"] == "001"
        # The overridden record keeps the incoming model's own ID, not the previous version's ID
        assert override_record["id"] == original_override_id
        assert override_record["id"] != first_record["id"]

        # Overriding again with no prior version in a fresh family behaves like a normal registration
        new_family_model = CalModel(cal_type="bias", datetime_obs=dt_obs)
        new_family_filepath, new_family_record = store.register_calibration(
            new_family_model, origin='LOCAL', override_latest=True
        )
        assert os.path.isfile(new_family_filepath)
        assert new_family_record["cal_version"] == "001"
        assert new_family_record["id"] == new_family_model.meta["id"]
        assert len(store.local_db) == 2