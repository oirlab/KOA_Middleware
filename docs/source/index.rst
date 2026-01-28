****************************
KOA Middleware Documentation
****************************

The KOA Middleware package provides a common interface for data pipelines to communicate with the Keck Observatory Archive (KOA). This documentation covers the current development version designed for the HISPEC and Liger DRPs (Data Reduction Pipelines).

Key Features
============

- **Retrieve** calibrations from KOA for data processing and cache them locally for efficient reuse.
- **Interface** for selecting the most appropriate calibration data for data processing with queries into a local SQLite database.
- **Environment Configuration**: Uses environment variables for configuring behavior.

See the :doc:`quickstart` guide to get started.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   authentication
   quickstart
   selectors
   api/api