import os
from typing import Sequence

from .utils import is_valid_uuid
from .selector_base import CalibrationSelector
from .database import LocalCalibrationDB, RemoteCalibrationDB
from .datamodel import AbstractCalibrationModel
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

        - Creates the `cache_dir`, `cache_dir/calibrations`, and `cache_dir/database`
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
    ...     cache_dir='/tmp/koa_cache',
    ...     local_database_filename='hispec_calibrations.db',
    ...     connect_remote=False
    ... )
    >>> # Initialize using environment variables (assuming they are set)
    >>> # os.environ['KOA_CALIBRATION_CACHE'] = '/tmp/koa_cache'
    >>> # store = CalibrationStore(instrument_name='hispec')
    """
    #### Initialization ####
    def __init__(
        self,
        instrument_name : str | None = None,
        cache_dir : str | None = None,
        local_database_filename : str | None = None,
        connect_remote : bool = True,
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
        """
        # Global control for using cached calibrations
        self.use_cache = get_env_var_bool('KOA_USE_CACHED_CALIBRATIONS', default=True)

        # Instrument name
        self.instrument_name = instrument_name

        # Initialize local cache and DB
        self._init_cache(cache_dir, local_database_filename)

        # Initialize the remote DB connection
        if connect_remote:
            self._init_remote_db()
        else:
            self.remote_db = None

    def _init_cache(self, cache_dir : str | None = None, local_database_filename : str | None = None):

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
            "KOA_CALIBRATION_CACHE environment variable must be set to a valid directory path" \
            " or pass a 'cache_dir' parameter."

        # Create cache directories
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(os.path.join(self.cache_dir, 'calibrations'), exist_ok=True)
        os.makedirs(os.path.join(self.cache_dir, 'database'), exist_ok=True)
        local_db_filepath = os.path.join(self.cache_dir, 'database', local_database_filename)
        table_name = os.environ.get('KOA_LOCAL_DATABASE_TABLE_NAME', f"{self.instrument_name.lower()}")
        self.local_db = LocalCalibrationDB(
            db_path=local_db_filepath,
            table_name=table_name,
        )

    def _init_remote_db(self):
        self.remote_db = RemoteCalibrationDB(self.instrument_name)
    
    ####################################
    #### Main API methods for users ####
    ####################################

    # Select and get calibration
    def select_and_get_calibration(
        self,
        input,
        selector : CalibrationSelector,
        return_record : bool = False,
        **kwargs
    ) -> str | tuple[str, dict]:
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

        Returns:
            If `return_record` is False (default):
                str: The local file path of the retrieved calibration file.
            If `return_record` is True:
                - `str`: The local file path of the retrieved calibration file.
                - `dict`: The record of the selected calibration from the local database.

        Example:
            >>> # Assuming 'my_input_data' and 'my_selector' are defined
            >>> local_filepath, calibration_record = store.select_and_get_calibration(my_input_data, my_selector, return_record=True)
            >>> print(f"Calibration file: {local_filepath}")
            >>> print(f"Calibration ID: {calibration_record['id']}")
        """
        result = selector.select(input, self.local_db, **kwargs)
        result = self.get_calibration(result, return_record=return_record)
        return result

    # Get a calibration and download if not cached
    def get_calibration(self, calibration: dict | str, return_record: bool = False) -> tuple[str, dict]:
        """
        Retrieves the calibration file based on its record or ID.
        Checks if the calibration is already cached locally, and downloads it if not.

        Parameters
        ----------
        calibration : dict | str
            A calibration metadata dictionary, calibration ID string, or local filepath string.
        """
        filepath_local = self.calibration_in_cache(calibration)
        if filepath_local is not None and self.use_cached:
            if return_record:
                cal_record = self.local_db.query_id(
                    calibration['id'] if isinstance(calibration, dict) else calibration
                )
                return filepath_local, cal_record
            return filepath_local
        logger.info("Calibration not found in cache, downloading...")
        local_filepath, cal_record = self.download_calibration(calibration, add_local=True)
        if return_record:
            return local_filepath, cal_record
        return local_filepath

    def calibration_in_cache(self, calibration: dict | str) -> str | None:
        """
        Checks if a calibration file is already present in the local cache.

        Parameters
        ----------
        calibration : dict | str
            A calibration metadata dictionary or calibration ID string.

        Returns
        -------
        str | None
            The absolute local file path if the calibration file is found in the cache, otherwise None.
        """
        if isinstance(calibration, dict):
            cal_id = calibration.get("id")
        elif isinstance(calibration, str):
            assert is_valid_uuid(calibration)
            cal_id = calibration
        else:
            msg = "Calibration must be a dict or str."
            logger.error(msg)
            raise TypeError(msg)
        cal_record = self.local_db.query(cal_id=cal_id)
        if not cal_record:
            return None
        return self._get_local_filepath(cal_record)

    def download_calibration(self, calibration: dict | str, add_local : bool = True) -> str:
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
            if add_local:
                cal_record = self.remote_db.query(cal_id=cal_id)
        else:
            msg = "Calibration must be a dict or str."
            logger.error(msg)
            raise TypeError(msg)
        output_dir = os.path.join(self.cache_dir, 'calibrations') + os.sep
        filepath_local = self.remote_db.download_calibration(
            cal_id=cal_id,
            output_dir=output_dir
        )
        if add_local:
            # NOTE: This is temporary until filenames are consistent.
            cal_record['filename'] = os.path.basename(filepath_local)
            self.local_db.add(cal_record)
        return filepath_local, cal_record

    def get_missing_entries(self, source : str = 'remote', mode : str = 'id') -> list[dict]:
        """
        Identifies calibration entries present in one database but missing from another.

        Parameters
        ----------
        source : str, optional
            The database to check for missing entries.
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
            msg = f"Invalid source '{source}' for get_missing_entries()."
            logger.error(msg)
            raise ValueError(msg)
        
        if mode == 'last_updated':
            last_updated_target = self.get_last_updated(source=target_name)
            calibrations = source_db.query(
                last_updated_start=last_updated_target  # strictly greater
            )
            return calibrations
        elif mode == 'id':
            cals_source = source_db.query()
            cals_target = target_db.query()
            ids_target = {cal['id'] for cal in cals_target}
            missing_cals = [cal for cal in cals_source if cal['id'] not in ids_target]
            return missing_cals
        else:
            msg = f"Invalid mode '{mode}' for get_missing_entries()."
            logger.error(msg)
            raise ValueError(msg)

    def register_local_calibration(
        self,
        calibration : dict | str | AbstractCalibrationModel
    ) -> tuple[str, dict]:
        """
        Registers a calibration to the local cache and metadata database.

        Parameters
        ----------
        calibration : dict | str | AbstractCalibrationModel
            A calibration metadata dictionary, filename string, or data model instance
            representing the calibration to register.
            If a datamodel instance, it must implement th AbstractCalibrationModel interface.
            If the calibration is a filepath, it will be copied into the local cache.

        Returns
        -------
        tuple[str, dict]
            A tuple containing:
                - `str`: The local file path where the calibration was saved.
                - `dict`: The calibration metadata dictionary as added to the database.
        """
        if isinstance(calibration, AbstractCalibrationModel):
            cal_record = calibration.to_record()
            output_dir = os.path.join(self.cache_dir, 'calibrations') + os.sep
            local_filepath = calibration.save(output_dir=output_dir)
        elif isinstance(calibration, dict):
            cal_record = calibration
            local_filepath = self._get_local_filepath(cal_record)
            # Assumed to be already saved.
        elif isinstance(calibration, str):
            pass
        cal_record_added = self.local_db.add(cal_record)
        return local_filepath, cal_record_added

    def sync_from_remote(self, mode : str = 'id') -> list[dict]:
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
        cals = self.get_missing_entries(source='remote', mode=mode)
        if len(cals) > 0:
            self.local_db.add(cals)
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

        Parameters
        ----------
        source : str | None
            Whether to query from the 'local' or 'remote' database. If None, defaults to 'remote' if available, otherwise 'local'.
        **kwargs
            Additional parameters to pass to the underlying ``query`` method.

        Returns
        -------
        list[dict] | dict | None
            Query results from the specified source.
        """
        if source is None:
            if self.remote_db is not None:
                source = 'remote'
            else:
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

    def sync_to_remote(self, mode : str = 'id') -> list[dict]:
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
        
        # !!!! TODO !!!! : Upload the calibration files first.
        
        
        # Get missing
        cals = self.get_missing_entries(source='remote', mode=mode)
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
                msg = "Calibration dictionary must contain 'filename' key."
                logger.error(msg)
                raise ValueError(msg)
        elif isinstance(calibration, str):
            filename = calibration
        else:
            msg = "Calibration must be a dict or str."
            logger.error(msg)
            raise TypeError(msg)
        return os.path.join(self.cache_dir, 'calibrations', filename)
    
    def populate_local_db_from_cache(self, calibrations : dict | AbstractCalibrationModel | Sequence[dict | AbstractCalibrationModel]) -> None:
        """
        Populates the local database from existing cached calibration files.

        Parameters
        ----------
        calibrations : dict | AbstractCalibrationModel | Sequence[dict | AbstractCalibrationModel]
            A single calibration metadata dictionary or a data model instance,
            or a list of these.

        Notes
        -----
        This method may be removed in the future if not found useful.
        """
        if isinstance(calibrations, (dict, AbstractCalibrationModel)):
            calibrations = [calibrations]
        cal_records = []
        for cal in calibrations:
            if isinstance(cal, AbstractCalibrationModel):
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

    #### Misc. ####
    def __repr__(self):
        return (
            f"{self.__class__.__name__}(\n"
            f"  instrument_name={self.instrument_name!r},\n"
            f"  local_db={self.local_db!r},\n"
            f"  remote_db={self.remote_db!r}\n"
            f")"
        )
