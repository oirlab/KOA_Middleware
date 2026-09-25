=====================
Calibration Selectors
=====================

Calibration selectors are Python classes that implement the logic to select a calibration from the local calibration database given input metadata from a DRP (Data Reduction Pipeline). All calibration selectors inherit from `CalibrationSelector`.

The local calibration database uses `sqlite-utils <https://sqlite-utils.datasette.io/>`_, a Python library that provides a convenient interface to SQLite databases. The sqlite-utils `Table <https://sqlite-utils.datasette.io/en/stable/reference.html#sqlite-utils-db-table>`_ (``store.local_db.table``) object provides direct access to sqlite-utils query methods.

How Selectors Work
==================

The selection process consists of the following three steps. Calling :py:meth:`~koa_middleware.selector_base.CalibrationSelector.select` executes this sequence in order.

1. **Get Candidates**: :py:meth:`~koa_middleware.selector_base.CalibrationSelector.get_candidates` queries the database for calibrations matching basic criteria.
2. **Select Best**: :py:meth:`~koa_middleware.selector_base.CalibrationSelector.select_best` chooses the best calibration using domain-specific logic.
3. **Fallback**: If no candidate is selected, :py:meth:`~koa_middleware.selector_base.CalibrationSelector.select_fallback` is called as a last resort.

Defining a Selector
===================

To create a custom selector, inherit from `CalibrationSelector` and implement the required method:

- ``get_candidates(input, db, **kwargs)``: Query the database and return a list of candidate calibration dictionaries that match basic criteria.

``db`` is an instance of `LocalCalibrationDB`.

See `CalibrationSelector` for more details on optional methods.

Queries use the sqlite-utils API to interact with the local calibration database. See the `sqlite-utils documentation <https://sqlite-utils.datasette.io/en/stable/python-api.html>`_ for details on available query methods. Below are common query patterns.

Example Selector
================

Below is an trimmed-down example selector for selecting dark calibrations for HISPEC, which has two spectrographs (BSPEC and RSPEC), each with their own detector.

The column **instrument_era** represents the stability periods of the instrument which might correspond to instrument servicing, etc.

.. code-block:: python

    from koa_middleware import CalibrationSelector, LocalCalibrationDB

    class DarkSelector(CalibrationSelector):
        """
        Selector for dark calibrations.
        """

        def get_candidates(self, meta, db : LocalCalibrationDB, **kwargs) -> list[dict]:
            """
            Retrieve candidate dark calibrations matching the input metadata.

            Parameters
            ----------
            meta : dict-like
                Input metadata from the observation.
            db : LocalCalibrationDB
                The koa_middleware local calibration database instance.
            """
            # Build SQL query for candidate calibrations
            sql = """
                cal_type = :cal_type
                AND instrument_era = :instrument_era
                AND spectrograph = :spectrograph
            """

            params = {
                'cal_type': 'dark',
                'instrument_era': meta['instrument_era'],
                'spectrograph': meta['spectrograph'],
            }

            # Fetch all matching rows from the database
            # Order by closest in time to input observation using mjd_start
            rows = list(db.table.rows_where(
                sql, params,
                order_by=f"ABS(mjd_start - {meta['mjd_start']})"
            ))

            return rows

Using a Selector
================

Once you've defined a selector, use it with the `CalibrationStore`:

.. code-block:: python

    from koa_middleware import CalibrationStore

    # Initialize the store
    with CalibrationStore(instrument_name='my_instrument') as store:
        
        # Create your selector
        selector = DarkSelector()

        # Define input metadata for your observation
        input_metadata = {
            'drp_version': '0.0.1',
            'spectrograph': 'BSPEC',
            'datetime_obs': '2024-09-24T12:00:00.000',
        }

        # Select and get the calibration in one step
        calibration_filepath = store.select_and_get_calibration(
            input_metadata,
            selector
        )

        # Or get both the file path and metadata
        calibration_filepath, calibration_record = store.select_and_get_calibration(
            input_metadata,
            selector,
        )
        
        print(f"Using calibration: {calibration_record['id']}")
        print(f"File location: {calibration_filepath}")

Query Patterns with sqlite-utils
=================================

The attribute ``LocalCalibrationDB.table`` provides access to sqlite-utils methods, and the `LocalCalibrationDB` class provides additional convenience methods. Here are common query patterns for ``get_candidates``:

Basic Queries
-------------

.. code-block:: python

    # Query with WHERE clause
    rows = list(db.table.rows_where(
        "cal_type = :type AND master_cal = 1",
        {"type": "dark"}
    ))

    # Query all rows
    all_rows = list(db.table.rows)

    # Query with ordering
    rows = list(db.table.rows_where(
        "cal_type = :type",
        {"type": "dark"},
        order_by=f"mjd_start DESC"
    ))

Conditional Queries
-------------------

.. code-block:: python

    # Range queries
    rows = list(db.table.rows_where(
        "mjd_start >= :min AND mjd_start <= :max",
        {"min": 60000.0, "max": 60100.0}
    ))

    # Multiple conditions
    rows = list(db.table.rows_where(
        """cal_type = :type 
           AND instrument_era = :era 
           AND datetime_obs > :date""",
        {"type": "dark", "era": "0.0.1", "date": "2024-01-01T00:00:00.000"}
    ))

Sorting and More Complex Queries
--------------------------------

Additional filtering can always be done after querying using Python built-ins.

.. code-block:: python

    # Get most recent calibration
    latest = db.table.rows_where(
        "cal_type = :type",
        {"type": "dark"},
        order_by=f"ABS(mjd_start - {meta['mjd_start']})", # Closest in time
        limit=1
    )

For complete sqlite-utils documentation, see https://sqlite-utils.datasette.io/en/stable/python-api.html