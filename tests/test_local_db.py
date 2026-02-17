import os
import uuid
import datetime

from koa_middleware.store import CalibrationStore


class CalibrationModel:
    """
    Lightweight local-only data model implementing save() and to_record().
    Recognized by AbstractCalibrationModel via structural typing.
    """
    def __init__(
        self,
        cal_type: str,
        datetime_obs: str,
        instrument_era: str = "era_1",
        spectrograph: str = "spectrograph_a",
        mjd_start: float = 60000.0,
        master_cal: int = 1,
    ):
        dt_utc = datetime.datetime.fromisoformat(datetime_obs)
        mjd_start = dt_utc.timestamp() / 86400.0 + 40587.0
        self.meta = {
            "id": str(uuid.uuid4()),
            "filename": f"cal_{cal_type}_{datetime_obs.replace(':', '').replace('-', '').replace('T', '_').replace('.', '')}.fits",
            "master_cal": master_cal,
            "cal_type": cal_type,
            "instrument_era": instrument_era,
            "spectrograph": spectrograph,
            "mjd_start": mjd_start,
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


def test_local_db(tmp_path):
    cache_dir = str(tmp_path)

    with CalibrationStore(
        instrument_name="test_instrument",
        cache_dir=cache_dir,
        local_database_filename=":memory:",
        connect_remote=False
    ) as store:

        # Ensure clean DB state
        store.local_db._reset(confirm=True)

        dt_before_register = datetime.datetime.now().isoformat(timespec='milliseconds')

        # Register 5 local calibrations
        base_dt = datetime.datetime(2024, 9, 24, 0, 0, 0)
        for i in range(5):
            dt = base_dt + datetime.timedelta(days=i)
            dt_obs = dt.isoformat()
            model = CalibrationModel(
                cal_type="dark",
                datetime_obs=dt_obs,
            )
            local_path, _ = store.register_calibration(model, origin='LOCAL')
            assert os.path.isfile(local_path), f"Calibration file not saved: {local_path}"

        dt_after_register = datetime.datetime.now().isoformat()

        # Query all local entries
        all_rows = store.local_db.query()
        assert isinstance(all_rows, list)
        assert len(all_rows) == 5

        # Query by datetime range
        dt_start = base_dt.isoformat()
        dt_end = (base_dt + datetime.timedelta(days=2)).isoformat()
        rows_dt = store.local_db.query(date_time_start=dt_start, date_time_end=dt_end)
        assert all(dt_start <= r["datetime_obs"] <= dt_end for r in rows_dt)

        # Query by last_updated range (start only)
        lu_start = (base_dt + datetime.timedelta(days=1)).isoformat()
        rows_lu = store.local_db.query(last_updated_start=lu_start)
        assert all(r["last_updated"] >= lu_start for r in rows_lu)

        # Verify get_last_updated
        last_updated = store.local_db.get_last_updated()
        assert isinstance(last_updated, str)
        #assert last_updated == (base_dt + datetime.timedelta(days=4)).isoformat()
        assert dt_before_register <= last_updated <= dt_after_register

        # Query by ID
        first_id = all_rows[0]["id"]
        row_by_id = store.local_db.query_id(first_id)
        assert row_by_id is not None and row_by_id["id"] == first_id

        # __len__ on LocalCalibrationDB
        assert len(store.local_db) == 5


def test_populate_local_db_from_cache(tmp_path):
    cache_dir = str(tmp_path)

    # Create dummy calibration files and metadata
    models = []
    base_dt = datetime.datetime(2024, 9, 24, 0, 0, 0)
    N = 3
    for i in range(N):
        dt = base_dt + datetime.timedelta(days=i)
        dt_obs = dt.isoformat()
        model = CalibrationModel(
            cal_type="flat",
            datetime_obs=dt_obs,
        )
        model.save(output_dir=cache_dir)
        models.append(model)

    with CalibrationStore(
        instrument_name="test_instrument",
        cache_dir=cache_dir,
        local_database_filename=":memory:",
        connect_remote=False
    ) as store:

        # Ensure clean DB state
        store.local_db._reset(confirm=True)

        # Populate local DB from existing cache
        store.populate_local_db_from_cache(models)

        # Verify entries in local DB
        all_rows = store.local_db.query()
        assert len(all_rows) == N
        for model in models:
            matching = [r for r in all_rows if r["id"] == model.meta["id"]]
            assert len(matching) == 1

        # Ensure clean DB state
        store.local_db._reset(confirm=True)

        # Populate local DB from existing cache
        store.populate_local_db_from_cache(models)

        # Verify entries in local DB
        all_rows = store.local_db.query()
        assert len(all_rows) == N
        for model in models:
            matching = [r for r in all_rows if r["id"] == model.meta["id"]]
            assert len(matching) == 1