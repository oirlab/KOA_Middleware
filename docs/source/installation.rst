Installation
============

This guide will walk you through setting up the KOA Middleware for development and use.

Prerequisites
-------------
Ensure you have `uv` installed. If not, you can install it by following the instructions on the `uv documentation <https://astral.sh/uv/tutorial/installation/>`_.

Development Environment Setup
-----------------------------

1.  **Clone the Repository:**

    First, clone the KOA Middleware repository from GitHub:

    .. code-block:: bash

        git clone https://github.com/oirlab/KOA_Middleware.git
        cd KOA_Middleware

2.  **Create and Activate a Virtual Environment:**

    It is highly recommended to use `uv` to create a virtual environment for dependency management:

    .. code-block:: bash

        uv venv
        source .venv/bin/activate

    (On Windows, use `.venv\Scripts\activate` instead of `source .venv/bin/activate`)

3.  **Install the Package:**

    With your virtual environment activated, install the KOA Middleware package along with its development dependencies:

    .. code-block:: bash

        uv pip install -e ".[test,docs]"

    This command installs the package in editable mode (`-e`), which means any changes you make to the source code will be immediately reflected without needing to reinstall. It also includes the `test` and `docs` optional dependencies.

Usage
-----
Once installed, you can import and use the `koa_middleware` package in your Python projects.
