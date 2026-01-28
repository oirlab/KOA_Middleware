from typing import Sequence
from contextlib import contextmanager
from datetime import datetime

from sqlite_utils import Database
from sqlite_utils.db import NotFoundError

from ..logging_utils import logger

__all__ = ["LocalCalibrationDB"]

_MIN_SCHEMA = {
    "id": str,
    "filename": str,
    "cal_type": str,
    "datetime_obs": str,
}


class LocalCalibrationDB:
    """
    Class to interact with a local SQLite calibration database using sqlite-utils.
    
    This class provides a simple interface for adding, querying, and managing calibration
    metadata stored as dictionaries in a SQLite database.
    """

    def __init__(self, db_path: str, table_name: str):
        """
        Initialize a LocalCalibrationDB instance.
        
        Parameters
        ----------
        db_path : str
            Path to the SQLite database file.
        table_name : str
            Name of the table to use for storing calibration metadata.
        """
        self.db_path = db_path
        self.table_name = table_name
        self.db = Database(db_path)

        if not self.table.exists():
            self.table.create(
                _MIN_SCHEMA,
                pk="id",
            )

    @contextmanager
    def transaction(self):
        """
        Context manager for database transactions.
        
        Ensures that changes are committed on success or rolled back on error.
        """
        try:
            with self.db.conn:
                yield
        except Exception:
            logger.exception("Transaction failed, rolling back.")

    def get_last_updated(self) -> str | None:
        """
        Get the most recent last_updated timestamp from the database.

        Returns
        -------
        str | None
            The maximum last_updated value as a string, or None if the table is empty.
        """
        row = next(
            self.db.execute(
                f"SELECT MAX(last_updated) AS v FROM {self.table_name}"
            ),
            None,
        )
        if len(row) == 0:
            logger.warning("No entries found in the calibration database.")
            return None
        if row[0] is None:
            logger.warning("No entries found in the calibration database.")
            return None
        return row[0]

    def custom_query(self, sql: str, params: tuple = ()) -> list[dict]:
        """
        Execute a custom SQL query.

        Parameters
        ----------
        sql : str
            The SQL query string.
        params : tuple, optional
            Parameters to pass to the SQL query.

        Returns
        -------
        list[dict]
            List of matching rows as dictionaries.
        """
        rows = self.db.execute(sql, params)
        return [dict(r) for r in rows]

    def query(
        self,
        cal_type: str | None = None,
        cal_id: str | None = None,
        date_time_start: str | None = None,
        date_time_end: str | None = None,
        last_updated_start: str | None = None,
        last_updated_end: str | None = None,
        order_by: str = 'last_updated',
        fetch: str = "all",
    ) -> list[dict] | dict | None:
        """
        Query calibration entries from the database with common use cases.

        Parameters
        ----------
        cal_type : str, optional
            Filter by calibration type.
        cal_id : str, optional
            Filter by a specific calibration UUID. Overrides other filters if provided.
        date_time_start : str, optional
            Minimum datetime_obs to include.
        date_time_end : str, optional
            Maximum datetime_obs to include.
        last_updated_start : str, optional
            Minimum last_updated timestamp to include.
        last_updated_end : str, optional
            Maximum last_updated timestamp to include.
        fetch : {'all', 'first'}, default 'all'
            Whether to return all matching rows or just the first one.

        Returns
        -------
        list[dict] or dict or None
            Matching calibration entries. If fetch='first', returns a single dict or None.
            If fetch='all', returns a list of dicts.
        """
        # Delegate to query_id() for single-ID queries
        if cal_id is not None:
            return self.query_id(cal_id)

        sql = ""
        params = {}

        if date_time_start is not None:
            sql += "datetime_obs >= :date_time_start AND "
            params["date_time_start"] = date_time_start

        if date_time_end is not None:
            sql += "datetime_obs <= :date_time_end AND "
            params["date_time_end"] = date_time_end

        if cal_type is not None:
            sql += "cal_type = :cal_type AND "
            params["cal_type"] = cal_type

        if last_updated_start is not None:
            sql += "last_updated >= :last_updated_start AND "
            params["last_updated_start"] = last_updated_start

        if last_updated_end is not None:
            sql += "last_updated <= :last_updated_end AND "
            params["last_updated_end"] = last_updated_end

        # Remove trailing " AND "
        sql = sql.rstrip(" AND ")

        if fetch == "first":
            rows = self.rows_where(sql if sql else None, params, limit=1, order_by=order_by)
            row = next(rows, None)
            return dict(row) if row else None

        return list(self.rows_where(sql if sql else None, params, order_by=order_by))
    
    def query_id(self, cal_id: str) -> dict | None:
        """
        Query a calibration entry by its unique ID.

        Parameters
        ----------
        cal_id : str
            The unique calibration ID (UUID).

        Returns
        -------
        dict or None
            The calibration metadata dictionary if found, otherwise None.
        """
        try:
            row = self.table.get(cal_id)
            return dict(row) if row else None
        except NotFoundError as e:
            return None

    def add(
        self,
        cals: dict | Sequence[dict],
        alter: bool = True,
        overwrite_last_updated: bool = False,
    ):
        """
        Add or update calibration entries in the database.

        Parameters
        ----------
        cals : dict or Sequence[dict]
            A single calibration metadata dictionary or a sequence of calibration 
            metadata dictionaries to add or update. Uses upsert semantics with 'id' as primary key.
        alter : bool, optional
            Whether to automatically alter the table schema to accommodate new fields.
            Default is True.
        """
        if isinstance(cals, dict):
            cals = [cals]
        items = list(cals)
        if not items:
            return
        
        # Set last updated timestamp
        # NOTE: Where this is performed may change in the future
        for item in items:
            if item.get("last_updated") is None or overwrite_last_updated:
                item["last_updated"] = datetime.now().isoformat(timespec='milliseconds')

        with self.transaction():
            logger.info(f"Adding {len(items)} calibration entries in local DB.")
            self.table.upsert_all(
                items,
                pk="id",
                alter=alter,
            )

    def delete(self, cal_id: str):
        """
        Delete a calibration entry by its unique ID.

        Parameters
        ----------
        cal_id : str
            The unique calibration ID (UUID) to delete.
        """
        try:
            self.table.delete(cal_id)
        except NotFoundError:
            logger.warning(f"Calibration ID {cal_id} not found in the database, cannot delete.")

    def _reset(self):
        """
        Reset the calibration database by dropping and recreating the table.
        WARNING: This will delete all existing calibration metadata in the DB.
        """
        if self.table.exists():
            self.table.drop()
        self.table.create(
            _MIN_SCHEMA,
            pk="id",
        )

    @property
    def table(self):
        """
        Returns the calibration table object.
        
        Returns
        -------
        sqlite_utils.db.Table
            The table object for the calibration metadata.
        """
        return self.db[self.table_name]
    
    def close(self):
        """
        Close the database connection.
        """
        self.db.close()

    def __repr__(self):
        return (
            f"LocalCalibrationDB(\n"
            f"    db_path={self.db_path!r},\n"
            f"    table_name={self.table_name!r},\n"
            f"    entries={self.table.count}\n"
            f"  )"
        )
    
    def __len__(self):
        """
        Return the number of entries in the calibration table.

        Returns
        -------
        int
            The number of calibration entries in the database.
        """
        return self.table.count
    
    @property
    def rows(self) -> list[dict]:
        """
        Get all rows in the calibration table.

        Returns
        -------
        Generator[dict]
            Generator of all calibration entries as dictionaries.
            Call list() on the result to get a list.
        """
        return self.table.rows
    
    @property
    def rows_where(self):
        """
        Forward function to sqlite-utils Table.rows_where method.
        """
        return self.table.rows_where