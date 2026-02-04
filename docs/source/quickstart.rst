==========
Quickstart
==========

Before starting, ensure the package is installed as described in the :doc:`installation` section, and activate the appropriate virtual environment.

Overview
========

The :py:class:`~koa_middleware.store.CalibrationStore` is the central component for managing calibration data and using selectors to retrieve the best calibrations. It has the following primary attributes:

- **local_db**: An instance of :py:class:`~koa_middleware.database.local_database.LocalCalibrationDB` for interacting with the local SQLite database of calibrations. ``store.local_db`` only manages the database of metadata. Calibration files are handled directly by the store.

- **remote_db**: An instance of :py:class:`~koa_middleware.database.remote_database.RemoteCalibrationDB` for interacting with the remote KOA calibration database.

- **cache_dir**: The directory path (string) where calibration files are cached locally.

Most interactions are performed through the :py:class:`~koa_middleware.store.CalibrationStore` interface, but local and remote DBs can be used directly as needed.

Basic Usage
-----------

The store is typically used as a context manager, and it must take in the ``instrument_name`` at minimum.

.. code-block:: python

    from koa_middleware import CalibrationStore

    with CalibrationStore(instrument_name='hispec', connect_remote=False) as store:
        # Perform operations with the store
        # Ex: store.get_calibration(...)
        ...

Environment Variables
---------------------

Additional parameters can be provided to :py:class:`~koa_middleware.store.CalibrationStore`. Configuration can also be set with environment variables (all prefixed with ``KOA_``). The location of the cache directory must be specified either through the ``CalibrationStore(cache_dir='<path>')`` parameter or the ``KOA_CALIBRATION_CACHE`` environment variable.

- **KOA_CALIBRATION_CACHE** (Required)
  Path to cached calibrations directory.

- **KOA_LOCAL_DATABASE_FILENAME** (Optional)
  Local SQLite database filename. Default: ``<instrument_name>_calibrations.db``

- **KOA_USE_CACHED_CALIBRATIONS** (Optional)
  Use cached files ('true' or 'false'). Default: 'true'.

- **KOA_LOCAL_DATABASE_TABLE_NAME** (Optional)
  Local database table name. Default: ``<instrument_name>``

- **KOA_CALIBRATIONS_URL** (Optional)
  Remote database URL. Default: Keck Observer API URL. Default is "https://www3.keck.hawaii.edu/api/calibrations", and will be replaced with the appropriate KOA URL in the future.

Calibration Cache Structure
---------------------------

In the case of two instruments HISPEC and Liger, the calibration cache directory is structured as follows:

.. code-block:: text

    /koa_calibration_cache/
        ├── calibrations/
        |   ├── hispec/
        │   ├────── hispec_cal1.fits
        │   ├────── hispec_cal2.fits
        │   └── ...
        |   ├── liger/
        │   ├────── liger_cal1.fits
        │   ├────── liger_cal2.fits
        │   └── ...
        └── database/
            └── hispec_calibrations.db
            └── liger_calibrations.db

The ``calibrations`` and ``database`` subdirectories are created automatically. A SQLite database file is created in the ``database`` subdirectory when the store is initialized if the specified database file does not already exist. Calibration files are stored in subdirectories named after the instrument within the ``calibrations`` directory and is also created automatically.

Calibration Data Structure
--------------------------

In python, calibrations are stored as dictionaries with at least the following entries. The key ``id`` serves as the primary key and must be unique for a given table.

.. code-block:: python

    calibration_meta = {
        'instrument_name' : 'HISPEC',                  # Instrument name (string).
        'id': 'b6fa2d86-45cf-5c6c-bf9b-7b8f7d0c3b9a', # Unique identifier (string). PRIMARY KEY.
        'filename': 'calibration_file.fits',          # Filename (string).
        'cal_type': 'dark',                           # Calibration type ('dark', 'flat', etc.)
        'datetime_obs': '2024-09-24T12:00:00.000',    # Observation datetime in ISO string. YYYY-MM-DDTHH:MM:SS.sss
    }

Additional fields can be included as needed.

Examples
========

Initialize the Calibration Store
--------------------------------

Create a ``CalibrationStore`` instance:

.. code-block:: python

    import os
    import datetime
    from koa_middleware.store import CalibrationStore
    from astropy.utils.data import _get_download_cache_loc

    # Set up a temporary cache directory for demonstration
    cache_dir = str(_get_download_cache_loc()) + os.sep + 'koa_calibration_cache' + os.sep
    os.makedirs(cache_dir, exist_ok=True)

    # Initialize the store with local database only
    with CalibrationStore(
        instrument_name='my_instrument',
        cache_dir=cache_dir,  # Alternatively, set KOA_CALIBRATION_CACHE env var
        connect_remote=False  # Local-only mode for most examples
    ) as store:
        # Print store details
        print(store)

Register Local Calibration
--------------------------

Add a new calibration record to the local SQLite database:

.. code-block:: python

    cal_record = {
        'id': '123e4567-e89b-12d3-a456-426614174000',
        'filename': 'hispec_dark.fits',
        'cal_type': 'dark',
        'instrument_era': '0.0.1',
        'spectrograph': 'BSPEC',
        'datetime_obs': '2024-09-24T12:00:00.000',
        'master_cal': 1, # True
    }

    with CalibrationStore(
        instrument_name='hispec',
        cache_dir=cache_dir,
        connect_remote=False
    ) as store:
        store.register_local_calibration(cal_record)


Retrieve a Known Calibration
----------------------------

Retrieve a calibration directly by ID, metadata record (dictionary), or filename:

.. code-block:: python

    from koa_middleware import CalibrationStore

    with CalibrationStore(instrument_name='hispec', cache_dir=cache_dir, connect_remote=False) as store:
        
        # By UUID
        local_filepath = store.get_calibration('12345678-90ab-cdef-1234-567890abcdef')
        
        # By metadata record
        local_filepath = store.get_calibration({
            'id': '12345678-90ab-cdef-1234-567890abcdef',
            'filename': 'dark.fits'
        })

        # By datamodel
        local_filepath, cal_record = store.get_calibration({
            'id': '12345678-90ab-cdef-1234-567890abcdef',
            'filename': 'dark.fits'
        })


Query Local Calibrations
------------------------

.. code-block:: python

    from koa_middleware import CalibrationStore

    with CalibrationStore(instrument_name='hispec', cache_dir=cache_dir, connect_remote=False) as store:

        # Query from local database for all dark calibrations in 2024
        result = store.query(
            cal_type='dark',
            date_time_start='2024-01-01T00:00:00',
            date_time_end='2024-12-31T23:59:59'
        )

        # Query from local database for all dark calibrations in 2024
        result = store.query(
            cal_id='12345678-90ab-cdef-1234-567890abcdef'
        )

        # store.local_db.table.rows_where() is also available for custom queries
        # The following is identical to above
        result = store.local_db.table.rows_where(
            "cal_type = :cal_type AND datetime_obs >= :start AND datetime_obs <= :end",
            {
                "cal_type": "dark",
                "start": "2024-01-01T00:00:00",
                "end": "2024-12-31T23:59:59"
            }
        )
        
        # Raw SQL if necessary
        result = store.local_db.custom_query(
            f"SELECT * FROM {store.local_db.table_name} WHERE cal_type = ?",
            ('dark',)
        )

Select and Retrieve with Selector
----------------------------------

Use a :py:class:`~koa_middleware.selector_base.CalibrationSelector` to find and retrieve the best calibration:

.. code-block:: python

    from koa_middleware import CalibrationStore

    with CalibrationStore(instrument_name='hispec', cache_dir=cache_dir, connect_remote=False) as store:
        selector = MyDarkSelector()

        # Define input metadata for calibration selection
        input_metadata = {
            'instrument_era': 'era_1',
            'spectrograph': 'spectrograph_a',
            'mjd_start': 60000.001,
        }

        # Select and retrieve in one step
        filepath, record = store.select_and_get_calibration(
            input_metadata,
            selector,
            return_record=True
        )
        
        print(f"Using calibration: {record['id']}")
        print(f"File location: {filepath}")

See :py:meth:`~koa_middleware.store.CalibrationStore.select_and_get_calibration` for full details.


Remote Operations
-----------------

Synchronize with remote database:

.. code-block:: python

    from koa_middleware import CalibrationStore

    with CalibrationStore(
        instrument_name='hispec',
        cache_dir=cache_dir,
        connect_remote=True
    ) as store:

        # Query remote database, same API as local
        result = store.query( # Or store.remote_db.query()
            source='remote',
            cal_type='flat',
            date_time_start='2024-01-01T00:00:00.000',
            date_time_end='2024-12-31T23:59:59.000'
        )

        # Sync local with remote
        # Syncs local SQLite DB with remote, no files are downloaded
        new_cals = store.sync_from_remote()

Calibration Selectors
=====================

Calibration selectors implement the logic to choose the best calibration from the database for your input data. See :doc:`selectors` for more information and how to implement custom selectors.

Next Steps
==========

- For complete API reference, see :py:class:`koa_middleware.store.CalibrationStore`, :py:class:`koa_middleware.database.local_database.LocalCalibrationDB`, and :py:class:`koa_middleware.database.remote_database.RemoteCalibrationDB`
