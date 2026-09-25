==============
Authentication
==============

The middleware can be used locally if you only need to access cached calibration files. However, to retrieve new calibrations from the Keck Observatory Archive (KOA), you will need to connect to the remote KOA calibration database. For now, this is hosted at Keck Observatory.

Keck Observer Account
---------------------

To access the remote KOA calibration database, you will need a Keck Observer account. See `here <https://www3.keck.hawaii.edu/newAccount>`_ to create an account.


Verification Token
++++++++++++++++++

The next step is to login to the Keck Observer portal. This will invoke a two-factor authentication (2FA) process, and a verification token will be sent to your email. Enter that token to the website to complete the login process.

Specifying Credentials
++++++++++++++++++++++

Once you have an account, set the following environment variables:

- **KECK_OBSERVER_EMAIL**: Your Keck Observer email address.
- **KECK_OBSERVER_PASSWORD**: Your Keck Observer password.

The recommended way to set these is using an environment file in your active working directory, and immediately load them using the `python-dotenv <https://pypi.org/project/python-dotenv/>`_ package.

In a file called ``.env`` in your working directory, add:

.. code-block:: text

    KECK_OBSERVER_EMAIL=your_email@example.com
    KECK_OBSERVER_PASSWORD=your_password

In your Python script, load the environment variables at the start:

.. code-block:: python

    from dotenv import load_dotenv

    # This will load the .env file, 
    # and set the environment variables,
    # They will be discarded after the script exits.
    load_dotenv()