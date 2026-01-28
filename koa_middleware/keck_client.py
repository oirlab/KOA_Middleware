import os
import json
from base64 import b64encode
from pathlib import Path

import requests
from platformdirs import user_state_dir

from .logging_utils import logger

__all__ = ["KeckObserverAuthClient"]

_APP_NAME = "koa_middleware"
_STATE_DIR = Path(user_state_dir(_APP_NAME))
_STATE_DIR.mkdir(parents=True, exist_ok=True)
_COOKIE_PATH = _STATE_DIR / "cookies.json"

_KECK_LOGIN_URL = "https://www3.keck.hawaii.edu"


class KeckObserverAuthClient:
    """
    Client for authorizing credentials to login to the Keck Observatory.
    
    This client manages authentication with the Keck Observatory login system,
    handling session management and cookie persistence. Eventually a similar client 
    will be made for the Keck Observatory Archive (KOA).
    """

    _cached_session = None
    _cached_observer_id = None

    def __init__(self):
        """
        Initialize the KeckObserverAuthClient.
        
        Attempts to load cached credentials from disk. If no valid cached credentials
        are found, performs a new login using KECK_EMAIL and KECK_PASSWORD environment variables.
        """
        self.login_url = _KECK_LOGIN_URL

        if KeckObserverAuthClient._cached_session is not None:
            self.session = KeckObserverAuthClient._cached_session
            self._observer_id = KeckObserverAuthClient._cached_observer_id
        else:
            self.session = requests.Session()
            self._observer_id = None
            
            if not self._load_cookies() or not self._validate_login():
                logger.info("No valid login detected, logging in...")
                oid, obs_cookie = self._perform_login()
                for k, v in obs_cookie.items():
                    self.session.cookies.set(k, v)
                self._observer_id = oid
                logger.info(f"Keck Observer login successful! Observer ID = {oid}")

            KeckObserverAuthClient._cached_session = self.session
            KeckObserverAuthClient._cached_observer_id = self._observer_id

    ###############
    #### Login ####
    ###############

    def _validate_login(self) -> bool:
        """
        Validate that the current session has valid login credentials.
        
        Returns
        -------
        bool
            True if the session is valid, False otherwise.
        """
        r = self.session.get(f"{self.login_url}/userinfo/odb-cookie")
        if r.status_code != 200:
            return False
        try:
            return r.json().get("Id") is not None
        except Exception:
            return False

    def _perform_login(self) -> tuple[str, dict]:
        """
        Perform login to the Keck Observatory using credentials from environment variables.
        
        Requires KECK_EMAIL and KECK_PASSWORD environment variables to be set.

        Returns
        -------
        tuple[str, dict]
            A tuple containing:
            - The observer ID as a string
            - The observer cookie dictionary

        Raises
        ------
        ValueError
            If KECK_EMAIL or KECK_PASSWORD are not set, or if login fails.
        RuntimeError
            If the account requires email verification (not supported).
        """
        email = os.getenv("KECK_OBSERVER_EMAIL")
        password = os.getenv("KECK_OBSERVER_PASSWORD")
        if not email or not password:
            msg = "KECK_OBSERVER_EMAIL and KECK_OBSERVER_PASSWORD must be set as environment variables."
            logger.error(msg)
            raise ValueError(msg)

        login_params = dict(email=email, password=password, url=self.login_url)
        r = requests.get(f"{self.login_url}/login/script", params=login_params)
        if r.status_code == 401:
            try:
                err = r.json()
            except Exception:
                err = {"comment": r.text}

            comment = err.get("comment", "").lower()
            if "verification" in comment:
                msg = "Account requires email verification code; token flow not supported here."
                logger.error(msg)
                raise RuntimeError(msg)
            error_msg = err.get("comment", "Invalid credentials.")
            logger.error(f"Login failed: {error_msg}")
            raise ValueError(error_msg)

        api = r.json()
        uid_cookie = {"KECK-AUTH-UID": api["py_uid"]}

        u = requests.get(f"{self.login_url}/userinfo/odb-cookie", cookies=uid_cookie)
        assert u.status_code == 200, f"{u} not successful"
        logger.info(f"User info request successful: {u.json()}")

        observer_id = str(u.json()["Id"])
        encoded = b64encode(observer_id.encode()).decode()
        observer_cookie = {"observer": f"obsid={encoded}"}

        _COOKIE_PATH.write_text(json.dumps(observer_cookie))

        return observer_id, observer_cookie

    @property
    def cookies_dict(self) -> dict:
        """
        Get the session cookies as a dictionary.
        
        Returns
        -------
        dict
            The cookies from the current session as a dictionary.
        """
        return requests.utils.dict_from_cookiejar(self.cookies)
    
    @property
    def cookies(self):
        """
        Get the session cookies.
        
        Returns
        -------
        requests.cookies.RequestsCookieJar
            The cookies from the current session.
        """
        return self.session.cookies

    def _load_cookies(self) -> bool:
        """
        Load cookies from disk if they exist.
        
        Returns
        -------
        bool
            True if cookies were successfully loaded, False otherwise.
        """
        if not _COOKIE_PATH.exists():
            return False
        try:
            data = json.loads(_COOKIE_PATH.read_text())
        except Exception:
            return False
        for k, v in data.items():
            self.session.cookies.set(k, v)
        return True