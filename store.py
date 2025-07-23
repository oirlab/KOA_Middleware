import os
from .selector_base import CalibrationSelector
from .database import LocalCalibrationDB, RemoteCalibrationDB, CalibrationORM

__all__ = ['CalibrationStore']


class CalibrationStore:
    """
    A CalibrationStore is used to manage storing, and retrieving calibrations.
    """

    _DEFAULT_KOA_CALIBRATION_DATABASE_URL = None # NOTE: Eventually change KOA PostgreSQL URL
    _DEFAULT_KOA_CALIBRATION_URL = None # NOTE: Eventually change KOA URL = "https://koa.ipac.caltech.edu/cgi-bin/getKOA/nph-getKOA?return_mode=json&
    
    # ENV VARS
    # KOA_CALIBRATION_CACHE
    #     REQUIRED: Path to top level directory for downloaded calibrations.
    # KOA_LOCAL_DATABASE_FILENAME
    #     OPTIONAL: Name of the local SQLite database file. Default is 'orm_class.hispec_calibrations.db' for HISPEC and 'parvi_calibrations.db' for PARVI.
    # KOA_REMOTE_DATABASE_URL
    #     OPTIONAL: PostgreSQL URL for the remote database. Set to None for only local operations including PARVI.
    #     Default is None for now, eventually KOA URL once deployed.
    # KOA_CALIBRATION_URL
    #     OPTIONAL: URL where actual calibrations (FITS files) are stored. Set to None for only local operations including PARVI.
    #     Default is None for now, eventually KOA once deployed.

    def __init__(
        self,
        orm_class : type[CalibrationORM],
        cache_dir : str | None = None,
        local_database_filename : str | None = None,
        remote_database_url : str | None = None,
        calibrations_url : str | None = None,
        use_cached : bool | None = None
    ):
        """
        Initialize the CalibrationStore.

        Args:
            orm_class (type[ORMCalibration]): The ORM class to use for SQL queries.
            cache_dir (str | None): Directory to store cached calibrations. If None, uses the KOA_CALIBRATION_CACHE environment variable.
            local_database_filename (str | None): Name of the local SQLite database file. If None, uses KOA_LOCAL_DATABASE_FILENAME environment variable.
            use_cached (bool | None): If True, use cached calibrations if available. If False, always download from remote even if already cached. If None, defaults to the ENV var KOA_USE_CACHED_CALIBRATIONS. If not set, defaults to True.
        """
        self.orm_class = orm_class

        if use_cached is not None:
            self.use_cached = use_cached
        else:
            self.use_cached = os.environ.get('KOA_USE_CACHED_CALIBRATIONS', 'true').lower() != 'false'
        
        if cache_dir is not None:
            self.cache_dir = cache_dir
        else:
            self.cache_dir = os.environ.get('KOA_CALIBRATION_CACHE', None)
            assert self.cache_dir is not None, "KOA_CALIBRATION_CACHE environment variable must be set to a valid directory path."
        
        if calibrations_url is not None:
            self.calibrations_url = calibrations_url
        else:
            self.calibrations_url = os.environ.get('KOA_CALIBRATION_URL', self._DEFAULT_KOA_CALIBRATION_URL)

        self.init_cache(local_database_filename)
        self.init_remote_db(remote_database_url)

    def init_cache(self, local_database_filename : str | None = None):
        if local_database_filename is None:
            local_database_filename = os.environ.get('KOA_LOCAL_DATABASE_FILENAME')
            if local_database_filename is None:
                assert self.orm_class.instrument is not None, "ORM class must have an instrument attribute set."
                local_database_filename = f'{self.orm_class.instrument.lower()}_calibrations.db'
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(os.path.join(self.cache_dir, 'calibrations'), exist_ok=True)
        os.makedirs(os.path.join(self.cache_dir, 'database'), exist_ok=True)
        local_db_filepath = os.path.join(self.cache_dir, 'database', local_database_filename)
        self.local_db = LocalCalibrationDB(db_path=local_db_filepath, orm_class=self.orm_class)

    def init_remote_db(self, remote_database_url : str | None = None):
        remote_database_url = os.environ.get('KOA_REMOTE_CALIBRATION_URL', self._DEFAULT_KOA_CALIBRATION_DATABASE_URL)
        if remote_database_url is not None:
            self.remote_db = RemoteCalibrationDB(url=remote_database_url)
        else:
            self.remote_db = None

    def _get_calibration(self, calibration, use_cached : bool | None = None) -> str:
        filepath_local = self.calibration_in_cache(calibration)
        if use_cached is None:
            use_cached = self.use_cached
        if filepath_local is not None and use_cached:
            return filepath_local
        else:
            return self.download_calibration(calibration)
    
    def get_calibration(
        self,
        input,
        selector : CalibrationSelector,
        use_cached : bool | None = None,
        **kwargs
    ) -> str:
        """
        Select the best calibration(s) based on the input data and a selection rule, download if not already cached.
        
        Args:
            input: Input data product.
            selector: A CalibrationSelector instance that defines the selection rule.
            use_cached: If True, return the cached calibration if available.
            kwargs: Additional parameters to pass to Selector.select().
        
        Returns:
            Selected calibration file(s).
        """
        result = selector.select(input, self.local_db, **kwargs)
        return self._get_calibration(result, use_cached=use_cached)

    def download_calibration(self, calibration):
       # NOTE: Implement this once we are set up at Keck or KOA.
       raise NotImplementedError("Download calibration not implemented yet.")
    
    def calibration_in_cache(self, calibration) -> str | None:
        filepath_local = self.get_local_filename(calibration)
        if os.path.exists(filepath_local):
            return filepath_local
        else:
            return None
    
    def get_local_filename(self, calibration) -> str:
        return os.path.join(self.cache_dir, 'calibrations', calibration.filename)
    
    def close(self):
        """
        Close both databases.
        """
        if self.remote_db is not None:
            self.remote_db.close()
        if self.local_db is not None:
            self.local_db.close()
    
    def get_missing_local_entries(self) -> list:
        """
        Get missing local entries based on LAST_UPDATED.
        """
        # NOTE: Need to test this method once formal remote DB is configured.
        last_updated_local = self.local_db.get_last_updated()
        calibrations = self.remote_db.query(
            date_time_start=last_updated_local,
        )
        return calibrations

    def register_local_calibration(self, calibration):
        """
        Register a calibration that is now stored in the appropriate calibrations directory and add to the local SQLLite DB.
        """
        output_dir = os.path.join(self.cache_dir, 'calibrations') + os.sep
        calibration.save(output_dir=output_dir)
        cal_orm = self.orm_class.from_datamodel(calibration)
        return self.local_db.add(cal_orm)
    
    def sync_from_remote(self) -> list:
        """
        Download entries present in the remote DB which are missing from the local DB based on LAST_UPDATED.
        """
        calibrations = self.get_missing_entries()
        calibrations = [cal for cal in calibrations if cal is not None]
        if calibrations:
            self.local_db.add(calibrations)
        return calibrations
    
    def update_from_cache(self):
        """
        Utility method to update the local SQLite DB from pre-existing calibrations in the cache directory.
        """
        return self.local_db.update_from_cache(self.cache_dir)
    
    def __enter__(self):
        """
        Context manager entry method.
        """
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        """
        Context manager exit method.
        """
        self.close()