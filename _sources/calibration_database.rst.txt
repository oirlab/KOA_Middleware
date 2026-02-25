====================
Calibration Database
====================

Overview
--------

The local calibration database to stores calibration files and metadata. The metadata database is implemented in SQLite.


Calibration Versioning
----------------------

Each calibration file is associated with a version, which is determined by the metadata of the calibration file. Version numbers start at "001" and increment for each "new version of that same calibration" up to "999".

Version Family
++++++++++++++

From a versioning context, calibration files are determined to be otherwise "identical" if they have the same values in a subset of metadata called the "version family".

The method :py:meth:`~koa_middleware.store.CalibrationStore.get_version_family_values` returns a list of metadata keys that are used to determine the version family. By default, the version family includes the following metadata keys:

- **Calibration Type**: ``cal_type``
- **Date time of observation**: ``datetime_obs``

**If two calibration files have the same values for all of the version family metadata keys andhave the same origin, then they are considered to be in the same version family and must have a unique version number.**


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