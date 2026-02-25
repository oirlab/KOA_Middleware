import os
import shutil
from typing import Sequence

from .utils import is_valid_uuid, generate_md5_file
from .selector_base import CalibrationSelector
from .database import LocalCalibrationDB, RemoteCalibrationDB
from .datamodel_protocol import SupportsCalibrationIO
from .logging_utils import logger
from .utils import get_env_var_bool

__all__ = ['CalibrationStore']


class CalibrationStore:
    """
    Class to manage the storage, retrieval, and synchronization of calibration data between a local database and the remote archive.

    The CalibrationStore class provides a unified interface for interacting with both
    local (SQLite) and remote calibration databases. It handles caching of calibration 
    files, querying for specific calibrations, and synchronizing calibration metadata 
    between local and remote repositories.

    Constructing this class sets up the necessary directory structure for caching calibration files and initializes the `LocalCalibrationDB` instance for managing the local SQLite database.

        - Creates the `cache_dir`, `cache_dir/calibrations/<instrument_name>`, and `cache_dir/database`
            directories if they do not already exist.
            
        - Initializes `self.local_db` with a `LocalCalibrationDB` instance.
        - Initializes `self.remote_db` with a `RemoteCalibrationDB` instance (if connect_remote is True).

    Parameters
    ----------
    instrument_name : str | None
        The name of the instrument associated with the calibration data (e.g., 'hispec', 'liger').
    cache_dir : str | None
        The absolute path to the directory where calibration files and the local
        SQLite database will be stored. If None, uses the KOA_CALIBRATION_CACHE 
        environment variable. Required either as parameter or environment variable.
    local_database_filename : str | None
        The filename for the local SQLite database. If None, uses the 
        KOA_LOCAL_DATABASE_FILENAME environment variable. If that is also None, 
        defaults to `f'{instrument_name.lower()}_calibrations.db'`.
    connect_remote : bool, optional
        Set to False to skip initializing the remote database connection. Default is True.

    Environment Variables
    
    - KOA_CALIBRATION_CACHE (Required) Path to cached calibrations directory.

    - KOA_LOCAL_DATABASE_FILENAME (Optional) Local SQLite database filename. Default: <instrument_name>_calibrations.db

    - KOA_USE_CACHED_CALIBRATIONS (Optional) Use cached files ('true' or 'false'). Default: 'true'.

    - KOA_LOCAL_DATABASE_TABLE_NAME (Optional) Local database table name. Default: <instrument_name>

    - KOA_CALIBRATIONS_URL (Optional) Remote database URL. Default: Keck Observer API URL. Default is “https://www3.keck.hawaii.edu/api/calibrations”, and will be replaced with the appropriate KOA URL in the future.

    Examples
    --------
    >>> from koa_middleware import CalibrationStore
    >>> # Initialize with explicit parameters
    >>> store = CalibrationStore(
    ...     instrument_name='hispec',
    ...     cache_dir='/tmp/koa_cache/',
    ...     local_database_filename='hispec_calibrations.db',
    ...     connect_remote=False
    ... )
    >>> # Initialize using environment variables (assuming they are set)
    >>> os.environ['KOA_CALIBRATION_CACHE'] = '/tmp/koa_cache/'
    >>> store = CalibrationStore(instrument_name='hispec')
    """
    #### Initialization ####
    def __init__(
        self,
        instrument_name : str | None = None,
        cache_dir : str | None = None,
        local_database_filename : str | None = None,
        connect_remote : bool = True,
        use_cached : bool = None,
        origin : str | None = None,
        sync_on_init : bool = True,
    ):
        """

        Parameters
        ----------
        instrument_name : str | None
            The name of the instrument associated with the calibration data (e.g., 'hispec', 'liger').
        cache_dir : str | None
            The absolute path to the directory where calibration files and the local
            SQLite database will be stored. If None, uses the KOA_CALIBRATION_CACHE 
            environment variable. Required either as parameter or environment variable.
        local_database_filename : str | None
            The filename for the local SQLite database. If None, uses the 
            KOA_LOCAL_DATABASE_FILENAME environment variable. If that is also None, 
            defaults to `f'{instrument_name.lower()}_calibrations.db'`.
        connect_remote : bool, optional
            Set to False to skip initializing the remote database connection. Default is True.
        use_cached : bool | None, optional
            Whether to use cached calibration files if they exist locally.
            If None, reads from the KOA_USE_CACHED_CALIBRATIONS environment variable (default True).
        origin : str | None, optional
            The origin to register calibrations under and retrieve calibrations for.
        sync_on_init : bool, optional
            Whether to automatically synchronize records from the remote database upon initialization. Default is True.
        """
        # Global control for using cached calibrations
        if use_cached is not None:
            self.use_cached = use_cached
        else:
            self.use_cached = get_env_var_bool('KOA_USE_CACHED_CALIBRATIONS', default=True)

        # Origin
        if isinstance(origin, str):
            origin = origin.upper()
        if origin is None:
            origin = os.getenv('KOA_CALIBRATION_ORIGIN')
        self.origin = origin

        # Instrument name
        self.instrument_name = instrument_name

        # Initialize local cache and DB
        self._init_cache(cache_dir, local_database_filename)

        # Initialize the remote DB connection
        if connect_remote:
            self._init_remote_db()
        else:
            self.remote_db = None

        if sync_on_init and self.remote_db is not None:
            self.sync_records_from_remote()

    def _init_cache(
        self,
        cache_dir : str | None = None,
        local_database_filename : str | None = None,
    ):

        # Get local database filename
        if local_database_filename is None:
            local_database_filename = os.environ.get('KOA_LOCAL_DATABASE_FILENAME')
        if local_database_filename is None or local_database_filename == '':
            local_database_filename = f'{self.instrument_name.lower()}_calibrations.db'

        # Get cache directory
        if cache_dir is not None:
            self.cache_dir = cache_dir
        else:
            self.cache_dir = os.environ.get('KOA_CALIBRATION_CACHE', None)
            assert self.cache_dir is not None, \
            "KOA_CALIBRATION_CACHE environment variable must be set to a valid directory path or pass a 'cache_dir' parameter."

        # Create cache directories
        self.data_dir = os.path.join(
            self.cache_dir,
            'calibrations',
            self.instrument_name.lower(),
        ) + os.sep
        self.database_dir = os.path.join(
            self.cache_dir,
            'database',
        ) + os.sep
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.database_dir, exist_ok=True)
        local_db_filepath = os.path.join(
            self.cache_dir,
            'database',
            local_database_filename
        )
        table_name = os.environ.get(
            'KOA_LOCAL_DATABASE_TABLE_NAME',
            f"{self.instrument_name.lower()}"
        )
        self.local_db = LocalCalibrationDB(
            db_path=local_db_filepath,
            table_name=table_name,
        )

    def _init_remote_db(self):
        if RemoteCalibrationDB._credentials_available():
            self.remote_db = RemoteCalibrationDB(self.instrument_name)
        else:
            logger.info("KOA credentials not found, remote calibration DB will not be connected.")
            self.remote_db = None
    
    ####################################
    #### Main API methods for users ####
    ####################################

    # Select and get calibration
    def select_and_get_calibration(
        self,
        input,
        selector : CalibrationSelector,
        **kwargs
    ) -> tuple[str, dict]:
        """
        Selects the best calibration based on input data and a selection rule, then retrieves it.

        This method uses a `CalibrationSelector` to identify the most appropriate calibration
        for the given input data. Once selected, it retrieves the calibration file,
        downloading it if it's not already cached locally.

        Parameters
        ----------
        input
            The input data product for which a calibration is needed.
        selector : CalibrationSelector
            An instance of a ``CalibrationSelector`` class.
        **kwargs
            Additional parameters to pass to the ``selector.select()`` method.

        Returns
        -------
        tuple[str, dict]
            - `str`: The local file path of the retrieved calibration file.
            - `dict`: The record of the selected calibration from the local database.

        Example:
            >>> # Assuming 'my_input_data' and 'my_selector' are defined
            >>> local_filepath, calibration_record = store.select_and_get_calibration(my_input_data, my_selector)
            >>> print(f"Calibration file: {local_filepath}")
            >>> print(f"Calibration ID: {calibration_record['id']}")
        """
        result = selector.select(input, self.local_db, **kwargs)
        result = self.get_calibration(result)
        return result
    
    def register_calibration(
        self,
        cal : SupportsCalibrationIO,
        origin : str | None = None,
        new_version : bool = False,
    ) -> tuple[str, dict]:
        """
        Registers a calibration to the local cache and metadata database.

        Parameters
        ----------
        cal : SupportsCalibrationIO
            The datamodel object to register.
        origin : str, optional
            The origin to register the calibration under.
        new_version : bool, optional
            Whether to generate a new version for this calibration. If False, the method will check if a calibration with the same version family already exists in the cache and skip registration if so. Defaults to False.

        Returns
        -------
        tuple[str, dict]
            A tuple containing:
                - `str`: The local file path where the calibration was saved.
                - `dict`: The calibration metadata dictionary as added to the database.
        """

        if self.calibration_record_in_cache(cal, mode='id'):
            msg = f"Calibration already exists in cache: {cal}! Skipping registration."
            logger.warning(msg)
            return None, None
        
        if not new_version and self.calibration_record_in_cache(cal, mode='version-family'):
            msg = f"Calibration already exists in cache: {cal}! Skipping registration."
            logger.warning(msg)
            return None, None
        
        cal_record = cal.to_record()

        # Generate version and add to metadata
        if new_version:
            self.generate_calibration_version(cal_record, origin=origin)
        
        # Save to local cache
        local_filepath = cal.save(output_dir=self.data_dir)

        # Generate MD5 and add to record
        cal_record['file_md5'] = generate_md5_file(local_filepath)

        # Add new record to local DB
        cal_record_added = self.local_db.add(cal_record)

        return local_filepath, cal_record_added

    # Get a calibration and download if not cached
    def get_calibration(self, cal: dict | str) -> tuple[str, dict]:
        """
        Retrieves the calibration file based on its record or ID.
        Checks if the calibration is already cached locally, and downloads it if not.

        Parameters
        ----------
        cal : dict | str
            A calibration metadata dictionary, calibration ID string, or local filepath string.

        Returns
        -------
        result : tuple[str, dict]
            - `str`: The absolute local file path where the calibration file is stored.
            - `dict`: The calibration metadata dictionary as stored in the local database.
        """

        cal_record = self.calibration_record_in_cache(cal, mode='id')
        
        # In this case, cal is probably an ID someone found listed on KOA
        if cal_record is None:
            
            if self.remote_db is None:
                msg = f"Calibration record not found in cache for {cal}, and no remote DB connection available to retrieve it."
                logger.error(msg)
                raise ValueError(msg)
            
            logger.info(f"Calibration record not found in cache for {cal}. Checking Remote DB...")
            cal_id = cal['id'] if isinstance(cal, dict) else cal
            assert is_valid_uuid(cal_id), f"Invalid calibration ID: {cal_id}"
            cal_record_remote = self.remote_db.query(cal_id=cal_id)
            if cal_record_remote is not None:
                logger.info(f"Calibration record found in Remote DB for {cal_id}. Adding record to local DB.")
                self.local_db.add(cal_record_remote)
                cal_record = cal_record_remote
            else:
                msg = f"Calibration record not found in Remote DB for {cal_id}"
                logger.error(msg)
                raise ValueError(msg)
            
        else:

            logger.info(f"Calibration record found in local cache for {cal_record['filename']}.")

            local_filepath = self.calibration_file_in_cache(cal_record)

            if local_filepath is not None:
                return local_filepath, cal_record
            else:
                logger.info("Calibration not found in cache, downloading...")
                local_filepath = self.download_calibration_file(cal_record)

            return local_filepath, cal_record
    
    def get_missing_local_files(self) -> list[dict]:
        """
        Identifies all calibration files that are recorded in the local sqlite DB
        but are missing from the local cache directory.

        Parameters
        ----------
        instrument_name : str, optional
            The name of the instrument to check for missing files. If None,
            all instruments are checked.

        Returns
        -------
        list[dict]
            A list of calibration metadata dictionaries for calibrations
            that are missing from the local cache.
        """

        if len(self.local_db) == 0:
            return []

        missing_files = []
        for cal_record in self.local_db.rows_where():
            local_filepath = self._get_local_filepath(cal_record)
            if not os.path.isfile(local_filepath):
                missing_files.append(cal_record)

        return missing_files
    
    def calibration_file_in_cache(self, cal : dict | str | SupportsCalibrationIO, filename : str | None = None) -> str | None:
        """
        Checks if a calibration file is already present in the local cache.

        Parameters
        ----------
        cal : dict | str | SupportsCalibrationIO
            Can be one of:
                - `str` : A calibration ID string or filepath.
                - `dict` : A calibration metadata dict.
                - `SupportsCalibrationIO` : A calibration data model instance.
        filename : str | None
            The filename to check for. If None, the filename will be extracted from the input `cal` parameter.

        Returns
        -------
        filepath : str | None
            The absolute local file path if the calibration file is found in the cache, otherwise None.
        """
        if filename is None:
            if isinstance(cal, SupportsCalibrationIO):
                cal_record = cal.to_record()
                filename = cal_record.get("filename")
            elif isinstance(cal, dict):
                filename = cal.get("filename")
            elif isinstance(cal, str):
                filename = os.path.basename(cal)
            else:
                raise ValueError(
                    "Invalid input type for calibration. Must be a DataModel, dict, or filepath string."
                )
        
        local_filepath = os.path.join(self.data_dir, filename)
        if os.path.isfile(local_filepath):
            return local_filepath
        else:
            return None

    def calibration_record_in_cache(
        self,
        cal: dict | str | SupportsCalibrationIO,
        mode: str = 'id'
    ) -> dict | None:
        """
        Checks if a calibration is already present in the local cache.

        Parameters
        ----------
        cal : dict | str | SupportsCalibrationIO
            Can be one of:
                - `str` : A calibration ID string or filepath.
                - `dict` : A calibration metadata dict.
                - `SupportsCalibrationIO` : A calibration data model instance.

        mode : str
            The mode to check the cache. Can be one of:
                - 'id' : Check by calibration ID (cal_id), the primary key in the database.
                - 'version-family' : Check by the version family (cal_type, datetime_obs, master_cal, spectrograph) + version (cal_version).
                - 'md5' : Check by the MD5 checksum of the calibration file.

        Returns
        -------
        dict | None
            The calibration metadata record if found, otherwise None.
        """

        # Guard against empty DB
        if len(self.local_db) == 0:
            return None

        mode = mode.lower()

        # Check by ID
        if mode == 'id':
            return self._calibration_record_in_cache_id(cal)
        # Check by ID
        if mode == 'version-family':
            return self._calibration_record_in_cache_version_family(cal)
        if mode == 'md5':
            return self._calibration_record_in_cache_md5(cal)
        raise ValueError(
            f"Invalid mode: {mode}. Must be one of 'id', 'family+version', or 'md5'."
        )
    
    def _calibration_record_in_cache_id(self, calibration: dict | str | SupportsCalibrationIO) -> dict | None:
        """
        Checks if a calibration is already present in the local cache by its calibration ID.

        Parameters
        ----------
        calibration : dict | str | SupportsCalibrationIO
            Can be one of:
                - `str` : A calibration ID string.
                - `dict` : A record dict.
                - SupportsCalibrationIO : A calibration data model instance.
        
        Returns
        -------
        dict | None
            The calibration metadata record if found, otherwise None.
        """

        if len(self.local_db) == 0:
            return None

        if isinstance(calibration, str) and is_valid_uuid(calibration):
            cal_id = calibration
        elif isinstance(calibration, SupportsCalibrationIO):
            cal_id = calibration.to_record().get("id")
        elif isinstance(calibration, dict):
            cal_id = calibration["id"]
        else:
            raise ValueError(
                "Invalid input type for calibration. Must be a DataModel, dict, or filepath string."
            )
        return self.local_db.query(cal_id=cal_id)

    def _calibration_record_in_cache_filename(self, cal : dict | SupportsCalibrationIO):
        """
        Checks if a calibration is already present in the local cache by its filename.

        Parameters
        ----------
        cal : dict | SupportsCalibrationIO
            Can be one of:
                - `dict` : A calibration metadata record.
                - `SupportsCalibrationIO` : A calibration data model instance.

        Returns
        -------
        dict | None
            The calibration metadata record if found, otherwise None.
        """

        if len(self.local_db) == 0:
            return None
        
        if isinstance(cal, SupportsCalibrationIO):
            cal_record = cal.to_record()
            filename = cal_record.get("filename")
        elif isinstance(cal, dict):
            filename = cal.get("filename")
        else:
            raise ValueError(
                "Invalid input type for calibration. Must be a DataModel or dict."
            )
        
        return self.local_db.query(filename=filename)

    def _calibration_record_in_cache_version_family(
        self,
        cal : dict | SupportsCalibrationIO,
        include_version : bool = False
    ) -> dict | list[dict] | None:
        """
        Checks if a calibration is already present in the local cache by its version family values and optionally version.

        This is expected to have the same output as _calibration_record_in_cache_filename.

        Parameters
        ----------
        cal : dict | SupportsCalibrationIO
            Can be one of:
                - `dict` : A calibration metadata record.
                - `SupportsCalibrationIO` : A calibration data model instance.
        include_version : bool
            Whether or not to include the version (cal_version) in the check. If False, only check if any version exists and return them all. Defaults to False.

        Returns
        -------
        dict | list[dict] | None
            The calibration metadata record if found, otherwise None.
        """

        if len(self.local_db) == 0:
            return None
        
        if isinstance(cal, SupportsCalibrationIO):
            cal_record = cal.to_record()
            cal_version = cal_record.get('cal_version')
        elif isinstance(cal, dict):
            cal_record = cal
        else:
            raise ValueError(
                "Invalid input type for calibration. Must be a DataModel, dict, or filepath string."
            )

        # Construct SQL query for version family + version
        sql_parts = []
        params = {}

        family = self.get_version_family_values(cal_record)

        for key, value in family.items():
            sql_parts.append(f"{key} = :{key}")
            params[key] = value

        # Append val version to SQL query
        if include_version:
            sql_parts.append("cal_version = :cal_version")
            params["cal_version"] = cal_version

        # Join with AND
        sql = " AND ".join(sql_parts)

        # Query DB
        rows = list(self.local_db.rows_where(sql, params))

        if rows:
            if include_version:
                return dict(rows[0])
            else:
                return [dict(row) for row in rows]
        else:
            return None

    def download_calibration_file(
        self,
        calibration: dict | str,
    ) -> str:
        """
        Downloads a calibration file from the remote DB.
        This does not register the calibration in the local DB.
        Most use cases should use ``store.get_calibration()`` instead.

        Parameters
        ----------
        calibration : dict | str
            A calibration metadata dictionary or calibration ID string.

        Returns
        -------
        str
            The absolute local file path where the calibration file was downloaded.
        """
        if isinstance(calibration, dict):
            cal_record = calibration
            cal_id = cal_record.get("id")
        elif isinstance(calibration, str):
            assert is_valid_uuid(calibration)
            cal_id = calibration
        else:
            msg = "Calibration must be a dict or str."
            logger.error(msg)
            raise TypeError(msg)
        filepath_local = self.remote_db.download_calibration(
            cal_id=cal_id,
            output_dir=self.data_dir
        )
        return filepath_local

    def get_missing_records(self, source : str = 'remote', mode : str = 'id') -> list[dict]:
        """
        Identifies calibration entries present in one database but missing from another.

        Parameters
        ----------
        source : str, optional
            - 'remote' (default): Returns entries in remote DB but not in local DB.
            - 'local': Returns entries in local DB but not in remote DB.
        mode : str, optional
            The mode to determine which entries are considered missing.
            Options are:
            - 'id' (default): Entries whose IDs are not present in the target database.
            - 'last_updated': Entries with a `last_updated` timestamp greater than the most recent timestamp in the target database.

        Returns
        -------
        list[dict]
            A list of dictionaries of metadata representing entries
                that are in the source DB but not yet in the target DB.
        """
        source = source.lower()
        mode = mode.lower()
        
        if source == 'remote':
            source_db = self.remote_db
            target_db = self.local_db
            target_name = 'local'
        elif source == 'local':
            source_db = self.local_db
            target_db = self.remote_db
            target_name = 'remote'
        else:
            msg = f"Invalid source '{source}' for get_missing_records()."
            logger.error(msg)
            raise ValueError(msg)
        
        if mode == 'last_updated':
            last_updated_target = self.get_last_updated(source=target_name)
            calibrations = source_db.query(
                last_updated_start=last_updated_target  # strictly greater
            )
            return calibrations
        elif mode == 'id':
            # TODO: This is sub optimal, needs fixed once DB grows larger.
            # TODO: To fix this, add function to remote DB to query a particular column for the entire DB.
            # TODO: Add column : str | list[str] = None kwarg to remote_db.query which returns a column name if provided, or all columns if not.
            cals_source = source_db.query()
            cals_target = target_db.query()
            ids_target = {cal['id'] for cal in cals_target}
            missing_cals = [cal for cal in cals_source if cal['id'] not in ids_target]
            return missing_cals
        else:
            msg = f"Invalid mode '{mode}' for get_missing_records()."
            logger.error(msg)
            raise ValueError(msg)

    def sync_records_from_remote(self, mode : str = 'id') -> list[dict]:
        """
        Synchronizes the local database with the remote database.

        This method fetches entries from the remote database that are missing
        from the local database based on the ``mode`` parameter, see below.
        It then adds these missing entries to the local database.

        Parameters
        ----------
        mode : str, optional
            The mode to determine which entries are considered missing.
            Options are:
            
                - 'last_updated': Entries with a `last_updated` timestamp greater than the most recent timestamp in the local database.
                - 'id' (default): Entries whose IDs are not present in the local database.

        Returns
        -------
        cals: list[dict]
            A list of dictionaries representing calibration entries that were added to the local database during synchronization.
        """
        cals = self.get_missing_records(source='remote', mode=mode)

        if len(cals) > 0:
            cals = self.local_db.add(cals)
        return cals

    def get_last_updated(self, source : str | None = None, **kwargs) -> str | None:
        """
        Get the last updated timestamp for the instrument's calibration data.

        Parameters
        ----------
        source : str | None
            Whether to query from the 'local' or 'remote' database.
            If None, defaults to 'remote' if available, otherwise 'local'.
        **kwargs
            Additional parameters to pass to ``local_db.get_last_updated()`` or ``remote_db.get_last_updated()``.

        Returns
        -------
        str | None
            The last updated timestamp as a string, or None if no entries exist.
        """
        if source is None:
            if self.remote_db is not None:
                source = 'remote'
            else:
                source = 'local'
        source = source.lower()
        if source == 'local':
            return self.local_db.get_last_updated(**kwargs)
        elif source == 'remote':
            return self.remote_db.get_last_updated(**kwargs)
        else:
            msg = f"Invalid source '{source}' for get_last_updated()."
            logger.error(msg)
            raise ValueError(msg)

    def query(self, source : str | None = None, **kwargs) -> list[dict] | dict | None:
        """
        Query calibrations from local or remote database.

        Users can also query the local and remote databases directly using ``store.local_db.query()`` and ``store.remote_db.query()``.

        This method may be removed in the future if not found useful.

        Parameters
        ----------
        source : str | None
            Whether to query from the 'local' or 'remote' database. If None, defaults to 'local'.
        **kwargs
            Additional parameters to pass to the underlying ``query`` method.

        Returns
        -------
        list[dict] | dict | None
            Query results from the specified source.
        """
        if source is None:
            source = 'local'
        source = source.lower()
        if source == 'local':
            return self.local_db.query(**kwargs)
        elif source == 'remote':
            return self.remote_db.query(**kwargs)
        else:
            msg = f"Invalid source '{source}' for query()."
            logger.error(msg)
            raise ValueError(msg)

    def sync_records_to_remote(self, mode : str = 'id') -> list[dict]:
        """
        Uploads local calibration entires to the remote DB.

        Parameters
        ----------
        mode : str, optional
            The mode to determine which entries are considered missing.
            Options are:
            - 'last_updated': Entries with a `last_updated` timestamp greater than the most recent timestamp in the local database.
                
            - 'id' (default): Entries whose IDs are not present in the local database.

        Returns
        -------
        cals: list[dict]
            A list of dictionaries representing calibration entries that were added to the remote database during synchronization.
        """
        
        # !!!! TODO !!!! : Upload the calibration files before calling this function.
        
        # Get missing
        cals = self.get_missing_records(source='local', mode=mode)

        if len(cals) > 0:
            for cal in cals:
                self.remote_db.add(cal)

    #### Utility Methods ####
    def _get_local_filepath(self, calibration: dict | str) -> str:
        """
        Constructs the expected full local filepath for a given calibration.

        Parameters
        ----------
        calibration : dict | str
            Either a calibration metadata dictionary containing a 'filename' key,
            or a string representing the filename.

        Returns
        -------
        filepath : str
            The absolute local file path where the calibration file is expected to be stored.
        """
        if isinstance(calibration, dict):
            filename = calibration.get('filename')
            if filename is None:
                return None
                # msg = "Calibration dictionary must contain 'filename' key."
                # logger.error(msg)
                # raise ValueError(msg)
        elif isinstance(calibration, str):
            filename = calibration
        else:
            msg = "Calibration must be a dict or str."
            logger.error(msg)
            raise TypeError(msg)
        return os.path.join(self.data_dir, filename)
    
    def populate_local_db_from_cache(self, calibrations : dict | SupportsCalibrationIO | Sequence[dict | SupportsCalibrationIO]) -> None:
        """
        Populates the local database from existing cached calibration files.

        Parameters
        ----------
        calibrations : dict | SupportsCalibrationIO | Sequence[dict | SupportsCalibrationIO]
            A single calibration metadata dictionary or a data model instance,
            or a list of these.

        Notes
        -----
        This method may be removed in the future if not found useful.
        """
        if isinstance(calibrations, (dict, SupportsCalibrationIO)):
            calibrations = [calibrations]
        cal_records = []
        for cal in calibrations:
            if isinstance(cal, SupportsCalibrationIO):
                cal_records.append(cal.to_record())
            else:
                cal_records.append(cal)
        
        self.local_db.add(cal_records)

    #### Context Manager ####
    def close(self):
        """
        Closes the connections to the local DB.
        Currently nothing is done to close the remote DB.
        The Keck Login session is cached for re-use within the same python session.
        """
        self.local_db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    #### Versioning ####
    def generate_calibration_version(
        self,
        calibration : dict,
        origin : str | None = None
    ) -> str:
        """
        Generate the next calibration version ("001", "002", ...), scoped to
        the calibration's version family and origin.

        Parameters
        ----------
        calibration : dict
            The calibration record for which to generate the version. Must contain the necessary metadata fields to determine its version family (e.g. cal_type, datetime_obs, master_cal, spectrograph).
        origin : str | None, optional
            The origin to use for generating the version. If None, the origin
            from the calibration metadata will be used.

        Returns
        -------
        str
            The calibration version string
        """

        # Origin to generate a calibration version for.
        if origin is not None:
            origin = origin.upper()
        else:
            if origin is None:
                origin = calibration.get('origin') or self.origin

            assert origin is not None, "Origin must be specified either in the input calibration metadata or as an argument to this function."
            origin = origin.upper()

        calibration['origin'] = origin

        cal_version = self._get_next_calibration_version(calibration)

        MAX_VERSION = 999
        if int(cal_version) > MAX_VERSION:
            raise ValueError(f"Invalid calibration version: {cal_version}")

        return cal_version

    def get_version_family_column_names(self, cal_type : str):
        """
        Retrieves the column names for the version family attributes.
        By default, this includes 'cal_type' and 'datetime_obs', but subclasses should override this method to specify different or additional columns for different calibration types.

        Parameters
        ----------
        cal_type : str
            The type of calibration.
        """
        return ['cal_type', 'datetime_obs']
    
    def get_version_family_values(self, cal : dict) -> dict:
        """
        Retrieves the fields/values that determine whether or not a calibration requires a new version.

        Parameters
        ----------
        cal : dict
            A calibration metadata record. One key must be 'cal_type' to determine the calibration type and thus the version family fields.
        cal_type : str
            The type of calibration.

        Returns
        -------
        dict
             A dictionary containing only the keys/values for metadata that determines the version family.
        """
        cal_type = cal.get('cal_type')
        assert cal_type, "cal_type must be present in calibration metadata"
        colnames = self.get_version_family_column_names(cal_type=cal_type)
        vals = {colname: cal[colname] for colname in colnames if colname in cal}
        return vals
    
    def _get_next_calibration_version(
        self,
        cal : dict | SupportsCalibrationIO,
        origin : str | None = None
    ) -> str:
        """
        Generate the next calibration version string for a calibration,
        determined by its version family.

        Parameters
        ----------
        cal : dict | SupportsCalibrationIO
            The calibration metadata record for which to generate the version.
        origin : str, optional
            The origin to generate the version for. If None, uses self.origin.

        Returns
        -------
        str
            A unique calibration version string (zero-padded, e.g. "001").
        """

        # Guard against empty DB
        if len(self.local_db) == 0:
            return "001"
        
        if isinstance(cal, SupportsCalibrationIO):
            cal_record = cal.to_record()
        elif isinstance(cal, dict):
            cal_record = cal
        else:
            raise TypeError(f"Expected SupportsCalibrationIO or dict, got {type(cal)}")

        # Origin to generate a calibration version for.
        origin = origin or cal_record.get('origin') or self.origin

        assert origin is not None, "Origin must be specified either in the input calibration metadata or as an argument to this function."

        # Get the version family values for the input calibration
        family = self.get_version_family_values(cal_record)
        family.setdefault("origin", origin)

        # Query the DB for all calibrations in the same version family and get their versions
        sql_parts = []
        params = {}

        for key, value in family.items():
            sql_parts.append(f"{key} = :{key}")
            params[key] = value

        sql = " AND ".join(sql_parts)

        rows = self.local_db.rows_where(sql, params)

        versions = [
            int(row["cal_version"])
            for row in rows
            if row.get("cal_version") is not None
        ]

        next_version = max(versions, default=0) + 1
        return f"{next_version:03d}"
    
    def detect_version_issues(self):
        # Ensure no two entries in the same version family have the same version number
        bad_records = []
        for record in self.local_db.query():
            family = self.get_version_family_values(record)
            version = record['cal_version']
            sql_parts = []
            params = {}
            for key, value in family.items():
                sql_parts.append(f"{key} = :{key}")
                params[key] = value
            sql_parts.append("cal_version = :cal_version")
            params["cal_version"] = version
            sql = " AND ".join(sql_parts)
            rows = list(self.local_db.rows_where(sql, params))
            if len(rows) > 1:
                bad_records.append(record)
                logger.warning(
                    f"Version issue detected: {len(rows)} calibrations found with the same version family and version number:\n"
                    f"Version family values: {family}\n"
                    f"Version number: {version}\n"
                    f"Calibration records: {[dict(row) for row in rows]}"
                )
        return bad_records
    
    def _reset_cache(self, confirm: bool = False, files : bool = True):
        """
        Reset the local calibration cache by clearing the local DB and optionally also deleting all files.

        This only applies the the current instrument.

        WARNING: This will permanently delete all cached calibration files. Use with caution.

        Parameters
        ----------
        confirm : bool
            A confirmation flag to prevent accidental deletion. Must be set to True to proceed with cache reset.
        files : bool
            Whether or not to also delete all cached calibration files. Defaults to True.
        """
        if not confirm:
            logger.warning("Cache reset not confirmed. Set confirm=True to proceed with cache reset.")
            return
        
        logger.info(f"Resetting local calibration DB for {self.instrument_name}")
        self.local_db._reset(confirm=confirm)

        if files:
            logger.info(f"Deleting all cached calibration {self.instrument_name} files...")
            if os.path.isdir(self.data_dir):
                shutil.rmtree(self.data_dir)
            os.makedirs(self.data_dir, exist_ok=True)
            

    #### Misc. ####
    def __repr__(self):
        return (
            f"{self.__class__.__name__}(\n"
            f"  instrument_name={self.instrument_name!r},\n"
            f"  local_db={self.local_db!r},\n"
            f"  remote_db={self.remote_db!r}\n"
            f")"
        )