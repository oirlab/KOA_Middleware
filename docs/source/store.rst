=================
Calibration Store
=================

The class :py:class:`~koa_middleware.store.CalibrationStore` manages the local (Keck or DRP user) and remote (KOA) databases. It is typically used as a context manager:

.. code-block:: python

    from koa_middleware import CalibrationStore

    with CalibrationStore(*args, **kwargs) as store:
        # Perform operations with the store
        pass

Input Arguments
---------------

The :py:class:`~koa_middleware.store.CalibrationStore` constructor accepts the following arguments:

- **orm_class** : ``type[ORMCalibration]``
    The ORM class to use for SQL queries.
- **cache_dir** : ``str | None``
    Directory to store cached calibrations. If None, uses the KOA_CALIBRATION_CACHE environment variable.
- **local_database_filename** : ``str | None``
    Name of the local SQLite database file. If None, uses KOA_LOCAL_DATABASE_FILENAME environment variable.
- **use_cached** : ``bool | None``
    If True, use cached calibrations if available. If False, always download from remote even if already cached. If None, defaults to the ENV var KOA_USE_CACHED_CALIBRATIONS. If not set, defaults to True.

Calibration Cache
-----------------

The calibration store uses the following local directory structure to store calibration files and a database with metadata for each calibration file:

.. code-block:: text
    :caption: Example calibration cache for HISPEC. The top level folder `hispec_calibration_cache` can be anything.

    /hispec_calibration_cache/
        ├── calibrations/
        │   ├── calibration_filename1.fits
        │   ├── calibration_filename2.fits
        │   ├── calibration_filename3.fits
        │   └── ...
        └── database/
            └── hispec_calibrations.db

The sub-folders `calibrations` and `database` will be created if they do not exist. By default, the local SQLite database is named ``f'{instrument_name}_calibrations.db'`` and is located in the ``database/`` subdirectory. The calibration files are located in the ``calibrations/`` subdirectory.


Environment Variables
---------------------

The following environment variables can be set to configure the calibration store:

- **KOA_CALIBRATION_CACHE**: Required for normal operations. Path to the top-level directory for downloaded calibrations. If not provided, the cache_dir must be specified when creating the calibration store.
- **KOA_LOCAL_DATABASE_FILENAME**: Optional. Name of the local SQLite database file. Default is ``f'{instrument.lower()}_calibrations.db'``.
- **KOA_REMOTE_DATABASE_URL**: Optional. PostgreSQL URL for the remote database. Set to None for only local operations including PARVI. Default is None for now, eventually will point to KOA once deployed.
- **KOA_CALIBRATION_URL**: Optional. URL where actual calibrations (FITS files) are stored. Default is None for now, eventually will point to KOA once deployed.