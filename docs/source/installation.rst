Installation
============

This guide will walk you through setting up the KOA Middleware for development and use.

Prerequisites
-------------

You will need Python 3.12 or later, and it's highly recommended to create a virtual environment for your project. We recommend using `uv <https://astral.sh/uv/>`_.

Environment Setup
-----------------

1.  **Clone the repository:**

    First, clone the KOA Middleware repository from GitHub:

    .. code-block:: bash

        git clone https://github.com/oirlab/KOA_Middleware.git
        cd KOA_Middleware

2.  **Create and activate a virtual environment:**

    Use `uv` to create and manage your virtual environment named `<env_name>` with the desired Python version `<version>`:

    .. code-block:: bash

        uv venv <env_name> --python <version>
        source <env_name>/bin/activate

3.  **Install the package:**

    With your virtual environment activated, install the KOA Middleware package along with its optional dependencies:

    .. code-block:: bash

        uv pip install -e .

    This command installs the package in editable mode (``-e``), which means any changes you make to the source code will be immediately reflected without needing to reinstall.


Authentication for Remote Access
--------------------------------

The middleware can be used locally if you only need to access cached calibration files. However, to retrieve new calibrations from the Keck Observatory Archive (KOA), you will need to connect to the remote KOA calibration database. For now, this is hosted at Keck Observatory.

See :doc:`authentication` for instructions on setting up access to the remote database.

Usage
-----

Once installed, you can import and use the ``koa_middleware`` package in your Python projects.

For more detailed usage instructions, refer to the :doc:`quickstart` guide.
