====================
Calibration Database
====================

Overview
--------

The local calibration database to stores calibration files and metadata. The metadata database is implemented in SQLite.

Required Columns
----------------

- **id** (str): A unique UUID4 identifier for the calibration file. This is the primary DB key.
- **filename** (str): The filename of the calibration file. Since filenames within the local cache are unique, this column must also be unique for each entry in the database.
- **datetime_obs** (str): The date and time of observation for the calibration file in ISOT format: YYYY-MM-DDTHH:MM:SS.SSS.
- **cal_type** (str): The type of calibration (e.g., "flat", "dark", "arc")
- **origin** (str): The origin of the calibration (e.g., "flat", "Keck") - see below.


last_updated column
+++++++++++++++++++

**Users do not need to add last_updated manually to any records.**

A special column called **last_updated** specifies when that record was added to the DB. It is automatically appended to a new record when adding it to the database.

- **last_updated** (str): The date and time when the calibration file was added to the DB in ISOT format: YYYY-MM-DDTHH:MM:SS.SSS.


Calibration Versioning
----------------------

Each calibration file is associated with a version, which is determined by the metadata of the calibration file. Version numbers start at "001" and increment for each "new version of that same calibration" up to "999".

Version Family
++++++++++++++

From a versioning context, calibration files are determined to be otherwise "identical" if they have the same values in a subset of metadata called the "version family".

In other words, if two calibration files have the same values for all of the version family metadata keys and have the same origin, then they are considered to be in the same version family and must have a unique version number.

Automatic versioning is largely supported. Users should override the method :py:meth:`~koa_middleware.store.CalibrationStore.get_version_family_values` to specify which metadata keys should be used to determine the version family.

The default version family metadata keys are:

- **datetime_obs** (str): The date and time of observation for the calibration file in ISOT format: YYYY-MM-DDTHH:MM:SS.SSS.
- **cal_type** (str): The type of calibration (e.g., "flat", "dark", "arc").
- **origin** (str): The origin of the calibration (e.g., "flat", "Keck") - see below.


Calibration Origin
------------------

**!! NOTE !! The behavior of *origin* is still being finalized subject to change. When creating new calibrations, populating the local/remote database, explicitly set the origin as needed.**

The origin defines the source of where the calibration came from, and provides an independent local-only namespace for assigning versions to calibration files.

- **KECK**: Calibrations generated at Keck which go through KOA.
- **LOCAL**: Calibrations that are generated outside of the main DRP processing at Keck and registered to the local cache.

It is stored in the FITS metadata and used during version generation and file registration. One can set this value globally with the ``KOA_CALBRATION_ORIGIN`` environment variable.

Important methods that interact with the origin include:

.. automethod:: koa_middleware.store.CalibrationStore.register_calibration
    :noindex:

.. automethod:: koa_middleware.store.CalibrationStore.generate_calibration_version
    :noindex: