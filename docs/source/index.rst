****************************
KOA Middleware Documentation
****************************

The KOA Middleware package specifies a common interface for data pipelines to communicate with the Keck Observatory Archive (KOA). This site will be under active development as the HISPEC and Liger DRPs (Data Reduction Pipelines) are created.

Purpose and Overview
====================

The KOA Middleware is designed to streamline the interaction between DRPs and the Keck Observatory Archive (KOA). Its primary purpose is to provide a robust and standardized way to:

*   **Manage Calibration Data:** Efficiently store, retrieve, and synchronize calibration files from both local and remote databases.
*   **Abstract Database Interactions:** Offer a unified interface for interacting with various calibration databases (e.g., SQLite for local, PostgreSQL for remote).
*   **Facilitate Data Access:** Simplify the process of accessing and utilizing KOA data within DRPs, ensuring data consistency and availability.

The middleware handles complexities such as caching, database querying, and data synchronization, allowing DRP developers to focus on scientific analysis rather than data management intricacies.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   quickstart
   installation
   store
   database
   selectors
   api/api