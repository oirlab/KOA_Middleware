import os
from .selector_base import CalibrationSelector
from .database import LocalCalibrationDB, RemoteCalibrationDB, CalibrationORM
import warnings

__all__ = ['CalibrationStore']


class CalibrationStore:
    """
    Manages the storage, retrieval, and synchronization of calibration data.

    The `CalibrationStore` class provides a unified interface for interacting with both
    local (SQLite) and remote (PostgreSQL) calibration databases. It handles caching
    of calibration files, querying for specific calibrations, and synchronizing
    calibration metadata between local and remote repositories.

    It relies on environment variables for default configurations, such as cache
    directory and database URLs, but these can be overridden during initialization.

    Attributes:
        orm_class (type[CalibrationORM]): The SQLAlchemy ORM class used for database queries.
        use_cached (bool): If True, cached calibrations are used; otherwise, calibrations are always downloaded.
        cache_dir (str): The base directory for storing cached calibration files and the local database.
        calibrations_url (str | None): The URL from which calibration files can be downloaded.
        local_db (LocalCalibrationDB): An instance of the local SQLite database handler.
        remote_db (RemoteCalibrationDB | None): An instance of the remote PostgreSQL database handler, if configured.
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
        Initializes a new instance of the CalibrationStore.

        This constructor sets up the local and, optionally, remote database connections,
        configures the caching directory, and determines whether to use cached calibrations.
        Environment variables can be used to provide default values for many parameters.

        Args:
            orm_class (type[CalibrationORM]): The SQLAlchemy ORM class that defines the structure
                                              of the calibration data. This class is used for
                                              database queries and interactions.
            cache_dir (str | None): The absolute path to the directory where calibration files
                                    and the local SQLite database will be stored. If `None`,
                                    the value of the `KOA_CALIBRATION_CACHE` environment variable
                                    is used. This parameter is required either directly or via
                                    the environment variable.
            local_database_filename (str | None): The filename for the local SQLite database.
                                                  If `None`, the value of the `KOA_LOCAL_DATABASE_FILENAME`
                                                  environment variable is used. If that's also `None`,
                                                  a default filename based on the `orm_class` table name
                                                  is generated (e.g., `tablename_calibrations.db`).
            remote_database_url (str | None): The URL for the remote PostgreSQL database.
                                             If `None`, the value of the `KOA_REMOTE_CALIBRATION_URL`
                                             environment variable is used. If both are `None`,
                                             no remote database connection is established.
            calibrations_url (str | None): The base URL from which calibration files can be downloaded.
                                          If `None`, the value of the `KOA_CALIBRATION_URL` environment
                                          variable is used. If both are `None`, remote calibration
                                          downloading might not be fully functional.
            use_cached (bool | None): If `True`, the store will prioritize using locally cached
                                      calibration files if available. If `False`, it will always
                                      attempt to download calibrations from the remote source,
                                      even if a cached version exists. If `None`, the value of
                                      the `KOA_USE_CACHED_CALIBRATIONS` environment variable is used
                                      (parsed as `True` unless explicitly 'false' or '0'). If that's
                                      also `None`, it defaults to `True`.

        Raises:
            AssertionError: If `cache_dir` is `None` and `KOA_CALIBRATION_CACHE` environment
                            variable is not set.

        Example:
            >>> from koa_middleware.database.orm_base import Base
            >>> from sqlalchemy import Column, Integer, String
            >>> class MyCalibrationORM(Base):
            ...     __tablename__ = 'my_calibrations'
            ...     id = Column(Integer, primary_key=True)
            ...     name = Column(String)
            >>> # Initialize with explicit parameters
            >>> store = CalibrationStore(
            ...     orm_class=MyCalibrationORM,
            ...     cache_dir='/tmp/koa_cache',
            ...     local_database_filename='my_cal.db',
            ...     use_cached=True
            ... )
            >>> # Initialize using environment variables (assuming they are set)
            >>> # os.environ['KOA_CALIBRATION_CACHE'] = '/tmp/koa_cache_env'
            >>> # store_env = CalibrationStore(orm_class=MyCalibrationORM)
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
        """
        Initializes the local calibration cache and database.

        This method sets up the necessary directory structure for caching calibration
        files and initializes the `LocalCalibrationDB` instance for managing the
        local SQLite database.

        Args:
            local_database_filename (str | None): The desired filename for the local
                                                  SQLite database. If `None`, the method
                                                  will attempt to use the environment variable
                                                  `KOA_LOCAL_DATABASE_FILENAME` or generate
                                                  a default filename based on the ORM class.

        Side Effects:
            - Creates the `cache_dir`, `cache_dir/calibrations`, and `cache_dir/database`
              directories if they do not already exist.
            - Initializes `self.local_db` with a `LocalCalibrationDB` instance.
        """
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
        """
        Initializes the connection to the remote calibration database.

        This method attempts to establish a connection to a remote PostgreSQL database
        using the provided URL or an environment variable. If no URL is provided
        (either directly or via environment variable), the `remote_db` attribute
        will be set to `None`.

        Args:
            remote_database_url (str | None): The URL for the remote PostgreSQL database.
                                             If `None`, the `KOA_REMOTE_CALIBRATION_URL`
                                             environment variable is checked.

        Side Effects:
            - Initializes `self.remote_db` with a `RemoteCalibrationDB` instance if a URL is available,
              otherwise sets it to `None`.
        """
        remote_database_url = os.environ.get('KOA_REMOTE_CALIBRATION_URL', self._DEFAULT_KOA_CALIBRATION_DATABASE_URL)
        if remote_database_url is not None:
            self.remote_db = RemoteCalibrationDB(url=remote_database_url)
        else:
            self.remote_db = None

    def _get_calibration(self, calibration : CalibrationORM, use_cached : bool | None = None) -> str:
        """
        Retrieves the calibration file based on its ORM instance.

        This internal method checks if the calibration is already cached locally.
        If it is and `use_cached` is `True`, the local path is returned. Otherwise,
        it attempts to download the calibration.

        Args:
            calibration (CalibrationORM): The ORM instance representing the calibration to retrieve.
            use_cached (bool | None): If `True`, returns the cached calibration if available.
                                      If `False`, always downloads from the remote even if already cached.
                                      If `None`, defaults to `self.use_cached`.

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
        Selects the best calibration based on input data and a selection rule, then retrieves it.

        This method uses a `CalibrationSelector` to identify the most appropriate calibration
        for the given input data. Once selected, it retrieves the calibration file,
        downloading it if it's not already cached locally.

        Args:
            input: The input data product for which a calibration is needed.
                   The type of this input depends on the specific `selector` used.
            selector (CalibrationSelector): An instance of a `CalibrationSelector` that defines
                                          the logic for selecting the best calibration from the database.
            use_cached (bool | None): If `True`, returns the cached calibration if available.
                                      If `False`, always downloads from the remote even if already cached.
                                      If `None`, defaults to `self.use_cached`.
            **kwargs: Additional parameters to pass to the `selector.select()` method.

        Returns:
            tuple[CalibrationORM, str]: A tuple containing:
                - `CalibrationORM`: The ORM instance representing the selected calibration.
                - `str`: The local file path of the retrieved calibration file.

        Example:
            >>> # Assuming 'my_input_data' and 'my_selector' are defined
            >>> # local_filepath, calibration_orm = store.get_calibration(my_input_data, my_selector)
            >>> # print(f"Calibration file: {local_filepath}")
            >>> # print(f"Calibration ORM ID: {calibration_orm.id}")
        """
        orm_result = selector.select(input, self.local_db, **kwargs)
        local_filepath = self._get_calibration(orm_result, use_cached=use_cached)
        return local_filepath, orm_result

    def get_calibration_by_id(self, calibration_id : str) -> tuple[CalibrationORM | None, str | None]:
        """
        Retrieves a calibration by its unique identifier.

        This method queries the local database for a calibration matching the given ID.
        If found, it retrieves the calibration file (downloading if necessary).

        Args:
            calibration_id (str): The unique identifier of the calibration to retrieve.

        Returns:
            tuple[CalibrationORM | None, str | None]: A tuple containing:
                - `CalibrationORM | None`: The ORM instance of the found calibration, or `None` if not found.
                - `str | None`: The local file path of the calibration file, or `None` if not found.

        Warns:
            UserWarning: If no calibration is found with the given ID.
            UserWarning: If multiple calibrations are found with the same ID (returns the first one).

        Example:
            >>> # local_filepath, calibration_orm = store.get_calibration_by_id('some_calibration_id')
            >>> # if calibration_orm:
            >>> #     print(f"Found calibration: {calibration_orm.id} at {local_filepath}")
            >>> # else:
            >>> #     print("Calibration not found.")
        """
        with self.local_db.session_manager() as session:
            calibration = self.local_db.query_by_id(calibration_id, session=session)
            if calibration is None or len(calibration) == 0:
                warnings.warn(f"No calibrations found with ID {calibration_id}, returning None")
                return None, None
            if len(calibration) > 1:
                warnings.warn(f"Multiple calibrations found with ID {calibration_id}, returning first found.")
            calibration = calibration[0]
            local_filepath = self._get_calibration(calibration)
            return local_filepath, calibration
            

    def download_calibration(self, calibration : CalibrationORM) -> str:
        """
        Downloads a calibration file from the remote URL.

        This method is responsible for fetching the actual calibration file (e.g., FITS file)
        from the configured remote URL and storing it in the local cache directory.

        Args:
            calibration (CalibrationORM): The ORM instance representing the calibration to download.

        Returns:
            str: The local file path of the downloaded calibration file.

        Raises:
            NotImplementedError: This method is currently under development and not yet implemented.
                                 It will be implemented once the remote KOA infrastructure is set up.
        """
        # NOTE: Implement this once we are set up at Keck or KOA.
        raise NotImplementedError("Download calibration not implemented yet.")
    
    def calibration_in_cache(self, calibration : CalibrationORM) -> str | None:
        """
        Checks if a calibration file is already present in the local cache.

        Args:
            calibration (CalibrationORM): The ORM instance representing the calibration to check.

        Returns:
            str | None: The absolute local file path of the calibration if it exists in the cache,
                        otherwise `None`.
        """
        filepath_local = self.get_local_filepath(calibration)
        if os.path.exists(filepath_local):
            return filepath_local
        else:
            return None
    
    def get_local_filepath(self, calibration : CalibrationORM) -> str:
        """
        Constructs the expected local file path for a given calibration ORM object.

        This method does not check for the existence of the file, only generates its path.

        Args:
            calibration (CalibrationORM): The ORM instance representing the calibration.

        Returns:
            str: The absolute local file path where the calibration file is expected to be stored.
        """
        return os.path.join(self.cache_dir, 'calibrations', calibration.filename)
    
    def close(self):
        """
        Closes the connections to both local and remote databases.

        This method calls `engine.dispose()` on the underlying SQLAlchemy engines
        for both `self.local_db` and `self.remote_db` (if they exist),
        releasing any held resources.
        """
        if self.remote_db is not None:
            self.remote_db.close()
        if self.local_db is not None:
            self.local_db.close()
    
    def get_missing_local_entries(self) -> list[CalibrationORM]:
        """
        Identifies calibration entries present in the remote database but missing from the local database.

        This method queries the remote database for entries that have been updated more recently
        than the last update recorded in the local database. It is intended to help synchronize
        the local cache with the remote source.

        Returns:
            list[CalibrationORM]: A list of `CalibrationORM` objects representing entries
                                  that are in the remote DB but not yet in the local DB.

        Raises:
            AttributeError: If `self.remote_db` is `None` (i.e., no remote database is configured).

        Note:
            This method is currently under development and requires a formal remote DB configuration
            to be fully functional and tested.
        """
        # NOTE: Need to test this method once formal remote DB is configured.
        last_updated_local = self.local_db.get_last_updated()
        calibrations = self.remote_db.query(
            date_time_start=last_updated_local,
        )
        return calibrations

    def register_local_calibration(self, calibration) -> tuple[str, CalibrationORM]:
        """
        Registers a calibration that has been saved to the local calibrations directory.

        This method takes a calibration object (expected to be a data model with a `save` method)
        and adds its corresponding ORM instance to the local SQLite database.

        Args:
            calibration: The calibration object to register. This object is expected to have
                         a `save(output_dir)` method that saves the calibration file and returns
                         its local path, and a `to_orm()` method that converts it to a
                         `CalibrationORM` instance.

        Returns:
            tuple[str, CalibrationORM]: A tuple containing:
            - `str`: The local file path where the calibration was saved.
            - `CalibrationORM`: The ORM instance representing the registered calibration,
                                as added to the local database.

        Note:
            This method assumes the input `calibration` object is a data model that handles
            its own saving to disk and conversion to an ORM object. Consider alternative
            approaches if this assumption changes.
        """
        output_dir = os.path.join(self.cache_dir, 'calibrations') + os.sep
        local_filepath = calibration.save(output_dir=output_dir)
        cal_orm = calibration.to_orm()
        self.local_db.add(cal_orm)
        return local_filepath, cal_orm
    
    def sync_from_remote(self) -> list[CalibrationORM]:
        """
        Synchronizes the local database with the remote database.

        This method fetches entries from the remote database that are missing from
        the local database (based on the `LAST_UPDATED` field) and adds them to the
        local database.

        Returns:
            list[CalibrationORM]: A list of `CalibrationORM` objects that were added
                                  to the local database during synchronization.

        Note:
            This method is currently under development and requires a formal remote DB configuration
            to be fully functional and tested.
        """
        calibrations = self.get_missing_local_entries()
        if len(calibrations) > 0:
            self.local_db.add(calibrations)
        return calibrations
    
    def __enter__(self):
        """
        Enters the runtime context related to this object.

        This method allows `CalibrationStore` instances to be used with the `with` statement,
        ensuring proper resource management.

        Returns:
            CalibrationStore: The instance itself.
        """
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exits the runtime context related to this object.

        This method is automatically called when exiting a `with` statement.
        It ensures that the database connections are properly closed by calling
        the `close()` method.

        Args:
            exc_type (type | None): The type of the exception that caused the context
                                    to be exited.
            exc_value (Exception | None): The exception instance that caused the context
                                          to be exited.
            traceback (TracebackType | None): A traceback object encapsulating the call stack
                                              at the point where the exception originally occurred.
        """
        self.close()