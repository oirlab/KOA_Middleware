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
                # NOTE: KEEP AN EYE ON HOW LOCAL DB FILENAMES ARE GENERATED
                local_database_filename = f'{self.orm_class.__tablename__.lower()}_calibrations.db'
        
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

    def _get_calibration(self, calibration : CalibrationORM, use_cached : bool | None = None) -> str:
        """
        Get the calibration file based on its ORM instance. Download if not already cached.

        Args:
            calibration (CalibrationORM): The ORM instance representing the calibration.
            use_cached (bool | None): If True, return the cached calibration if available. If False, always download from remote even if already cached. If None, defaults to self.use_cached.

        Returns:
            str: The local file path of the calibration file.
        """
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
    ) -> tuple[CalibrationORM, str]:
        """
        Select the best calibration based on the input data and a selection rule, download if not already cached.
        
        Args:
            input: Input data product.
            selector (CalibrationSelector): A CalibrationSelector instance that defines the selection rule.
            use_cached (bool | None): If True, return the cached calibration if available.
            kwargs: Additional parameters to pass to Selector.select().
        
        Returns:
            str: The local file path of the calibration file.
            CalibrationORM: The ORM instance and local filepath.
        """
        orm_result = selector.select(input, self.local_db, **kwargs)
        local_filepath = self._get_calibration(orm_result, use_cached=use_cached)
        return local_filepath, orm_result

    def get_calibration_by_id(self, calibration_id : str) -> tuple[CalibrationORM, str]:
        with self.local_db.session_manager() as session:
            calibration = self.local_db.query_by_id(calibration_id, session=session)
            if calibration is None:
                raise ValueError(f"Calibration with ID {calibration_id} not found in local database.")
            local_filepath = self._get_calibration(calibration)
            return calibration, local_filepath
            

    def download_calibration(self, calibration : CalibrationORM) -> str:
        """
        Download the calibration from the remote URL (*under development*).

        Args:
            calibration (CalibrationORM): The ORM instance representing the calibration to download.

        Returns:
            str: The local file path of the downloaded calibration file.
        """
        # NOTE: Implement this once we are set up at Keck or KOA.
        raise NotImplementedError("Download calibration not implemented yet.")
    
    def calibration_in_cache(self, calibration : CalibrationORM) -> str | None:
        """
        Check if the calibration file is already cached locally.

        Args:
            calibration (CalibrationORM): The ORM instance to check.

        Returns:
            str | None: The local file path if the calibration is cached, otherwise None.
        """
        filepath_local = self.get_local_filepath(calibration)
        if os.path.exists(filepath_local):
            return filepath_local
        else:
            return False
    
    def get_local_filepath(self, calibration) -> str:
        """
        Get the local filepath of a calibration ORM object.

        Args:
            calibration (CalibrationORM): The ORM instance.

        Returns:
            str: The local file path of the calibration.
        """
        return os.path.join(self.cache_dir, 'calibrations', calibration.filename)
    
    def close(self):
        """
        Close both databases by calling engine.dipose() on the local and remote DB.
        """
        if self.remote_db is not None:
            self.remote_db.close()
        if self.local_db is not None:
            self.local_db.close()
    
    def get_missing_local_entries(self) -> list[CalibrationORM]:
        """
        Identify entries in the remote DB that are missing from the local DB, 
        using the LAST_UPDATED field as a reference.
        *under development*

        Returns:
            list[CalibrationORM]: List of missing CalibrationORM objects.
        """
        # NOTE: Need to test this method once formal remote DB is configured.
        last_updated_local = self.local_db.get_last_updated()
        calibrations = self.remote_db.query(
            date_time_start=last_updated_local,
        )
        return calibrations

    def register_local_calibration(self, calibration) -> CalibrationORM:
        """
        Register a calibration that is now stored in the appropriate calibrations directory by adding it to the local SQLLite DB.
        
        NOTE: This method is responsible for calling ``model.save()``, so we must consider the input being a datamodel. Consider alternative approach.

        Args:
            calibration (DataModel): The calibration object to register.

        Returns:
            CalibrationORM: The ORM instance representing the registered calibration.
        """
        output_dir = os.path.join(self.cache_dir, 'calibrations') + os.sep
        calibration.save(output_dir=output_dir)
        cal_orm = calibration.to_orm()
        return self.local_db.add(cal_orm)
    
    def sync_from_remote(self) -> list[CalibrationORM]:
        """
        Download entries present in the remote DB which are missing from the local DB based on LAST_UPDATED.
        *under development*
        """
        calibrations = self.get_missing_local_entries()
        if len(calibrations) > 0:
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