from datetime import datetime, timezone, timedelta

import pytest

from koa_middleware.store import CalibrationStore
from koa_middleware.utils import datetime_to_isot_ms


class FakeRemoteDB:
    """
    Stands in for RemoteCalibrationDB so no external calls are made.
    """
    calibrations_url = "https://example.invalid/api/calibrations"

    def __init__(self):
        self.n_queries = 0
        self.records = [
            {"id": "11111111-1111-1111-1111-111111111111", "filename": "cal_1.fits"},
            {"id": "22222222-2222-2222-2222-222222222222", "filename": "cal_2.fits"},
        ]

    def query(self, **kwargs):
        self.n_queries += 1
        return list(self.records)


class FakeRemoteStore(CalibrationStore):
    """
    CalibrationStore that uses one FakeRemoteDB shared across instances.
    """
    remote = None

    def _init_remote_db(self):
        self.remote_db = type(self).remote


@pytest.fixture
def make_store(tmp_path, monkeypatch):
    monkeypatch.delenv("KOA_CALDB_SYNC_INTERVAL", raising=False)
    FakeRemoteStore.remote = FakeRemoteDB()

    def _make(**kwargs):
        with FakeRemoteStore(
            instrument_name="test_instrument",
            cache_dir=str(tmp_path),
            **kwargs,
        ) as store:
            return store

    return _make


def _syncs(tmp_path):
    with CalibrationStore(
        instrument_name="test_instrument",
        cache_dir=str(tmp_path),
        connect_remote=False,
    ) as store:
        return list(store.local_db.remote_syncs_table.rows)


def test_sync_on_init_default_uses_interval(make_store, tmp_path):
    remote = FakeRemoteStore.remote

    # Never synced: syncs and logs it
    make_store()
    syncs = _syncs(tmp_path)
    assert len(syncs) == 1
    assert syncs[0]["mode"] == "id"
    assert syncs[0]["n_added"] == 2
    assert syncs[0]["remote_url"] == FakeRemoteDB.calibrations_url
    n_queries = remote.n_queries

    # Within the interval: no remote query
    make_store()
    assert remote.n_queries == n_queries
    assert len(_syncs(tmp_path)) == 1


def test_sync_on_init_explicit(make_store, tmp_path):
    make_store()
    make_store(sync_on_init=True)
    assert len(_syncs(tmp_path)) == 2
    make_store(sync_on_init=False)
    assert len(_syncs(tmp_path)) == 2


def test_sync_interval_env_var(make_store, tmp_path, monkeypatch):
    monkeypatch.setenv("KOA_CALDB_SYNC_INTERVAL", "0")
    make_store()
    make_store()
    assert len(_syncs(tmp_path)) == 2

    monkeypatch.setenv("KOA_CALDB_SYNC_INTERVAL", "-1")
    make_store()
    assert len(_syncs(tmp_path)) == 2


def test_stale_sync_triggers_resync(make_store, tmp_path):
    make_store()

    # Backdate the only sync to two hours ago
    with CalibrationStore(
        instrument_name="test_instrument",
        cache_dir=str(tmp_path),
        connect_remote=False,
    ) as store:
        old = datetime_to_isot_ms(datetime.now(timezone.utc) - timedelta(hours=2))
        with store.local_db.db.conn:
            store.local_db.db.execute("UPDATE remote_syncs SET synced_at = ?", [old])
        assert store.remote_sync_is_stale()

    make_store()
    assert len(_syncs(tmp_path)) == 2


def test_failed_sync_not_logged(make_store, tmp_path):
    def fail(**kwargs):
        raise RuntimeError("Failed to query metadata")
    FakeRemoteStore.remote.query = fail

    with pytest.raises(RuntimeError):
        make_store()
    assert _syncs(tmp_path) == []


def test_reset_clears_sync_log(make_store, tmp_path):
    make_store()
    with CalibrationStore(
        instrument_name="test_instrument",
        cache_dir=str(tmp_path),
        connect_remote=False,
    ) as store:
        assert store.get_last_remote_sync_time() is not None
        store.local_db._reset(confirm=True)
        assert store.get_last_remote_sync_time() is None
        assert store.remote_sync_is_stale()
