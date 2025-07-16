import os
from hispecdrp.calibrations import HISPECCalibrationORM, PARVICalibrationORM
from .selector_base import CalibrationSelector
from .database import LocalCalibrationDB
from .database import RemoteCalibrationDB

from hispecdrp import datamodels

__all__ = ['get_calibration_store', 'CalibrationStore']

def get_calibration_store(
    input_model : datamodels.HISPECDataModel | str | None = None,
    instrument : str | None = None,
    cache_dir : str | None = None,
    database_filename : str | None = None,
) -> 'CalibrationStore':
    """
    Retrieve an instance of the CalibrationStore. Specify either an input model (HISPECDataModel or PARVICalibrationModel) or the instrument name ('hispec' or 'parvi').

    Args:
        input_model: An instance of HISPECDataModel or PARVICalibrationModel, or filepath. Defaults to None.
        instrument: A string indicating the instrument ('hispec' or 'parvi').  Defaults to None.
        cache_dir: Directory path for local calibration file cache.
        database_filename: Local filename for SQLite calibration database.
    """
    if input_model is not None:
        input_model = datamodels.open(input_model, meta_only=True)
        if isinstance(input_model, datamodels.HISPECDataModel):
            orm_class = HISPECCalibrationORM
            default_db_filename = 'hispec_calibrations.db'
        elif isinstance(input_model, datamodels.PARVIDataModel):
            orm_class = PARVICalibrationORM
            default_db_filename = 'parvi_calibrations.db'
        else:
            raise ValueError(f"Unknown input model type: {type(input_model)}. Expected HISPECCalibrationModel or PARVICalibrationModel.")
    elif instrument is not None:
        if instrument.lower() == 'hispec':
            orm_class = HISPECCalibrationORM
            default_db_filename = 'hispec_calibrations.db'
        elif instrument.lower() == 'parvi':
            orm_class = PARVICalibrationORM
            default_db_filename = 'parvi_calibrations.db'
        else:
            raise ValueError(f"Unknown instrument: {instrument}")
    else:
        raise ValueError("Either input_model or instrument must be provided to get_calibration_store.")

    # Resolve cache_dir and database_filename
    resolved_cache_dir = cache_dir or os.environ.get('HISPECDRP_CALIBRATION_CACHE')
    if resolved_cache_dir is None:
        raise ValueError("cache_dir must be provided or set in HISPECDRP_CALIBRATION_CACHE environment variable.")

    resolved_database_filename = database_filename or os.environ.get('HISPECDRP_CALIBRATION_DATABASE_FILENAME', default_db_filename)

    return CalibrationStore(
        remote_database_url=os.environ.get('HISPECDRP_CALIBRATION_DATABASE_URL', None),
        remote_calibration_url=os.environ.get('HISPECDRP_CALIBRATION_URL', None),
        cache_dir=resolved_cache_dir,
        orm_class=orm_class,
        local_database_filename=resolved_database_filename,
    )



class CalibrationStore:
    """
    A CalibrationStore is used to manage storing, and retrieving calibrations.
    """

    _DEFAULT_HISPECDRP_CALIBRATION_DATABASE_URL = None # NOTE: Eventually change KOA PostgreSQL URL
    _DEFAULT_HISPECDRP_CALIBRATION_URL = None # NOTE: Eventually change KOA URL = "https://koa.ipac.caltech.edu/cgi-bin/getKOA/nph-getKOA?return_mode=json&"
    
    # ENV VARS
    # HISPECDRP_CALIBRATION_CACHE
    #     REQUIRED: Path to top level directory for downloaded calibrations.
    # HISPECDRP_CALIBRATION_DATABASE_FILENAME
    #     OPTIONAL: Name of the local SQLite database file. Default is 'hispec_calibrations.db' for HISPEC and 'parvi_calibrations.db' for PARVI.
    # HISPECDRP_CALIBRATION_DATABASE_URL
    #     OPTIONAL: PostgreSQL URL for the remote database. Set to None for only local operations including PARVI.
    #     Default is None for now, eventually KOA once deployed.
    # HISPECDRP_CALIBRATION_URL
    #     OPTIONAL: URL where actual calibrations (FITS files) are stored. Set to None for only local operations including PARVI.
    #     Default is None for now, eventually KOA once deployed.

    def __init__(
        self,
        remote_database_url : str | None = None,
        remote_calibration_url : str | None = None,
        cache_dir : str | None = None,
        orm_class : type | None = None,
        local_database_filename : str | None = None,
    ):
        
        # ORM class for queries
        self.orm_class = orm_class

        # URL where actual calibrations (FITS files) are stored
        if remote_calibration_url is None:
            self.calibration_url = os.environ.get('HISPECDRP_CALIBRATION_URL', self._DEFAULT_HISPECDRP_CALIBRATION_URL)
        else:
            self.calibration_url = remote_calibration_url

        # Init the local cache
        self.init_cache(cache_dir, local_database_filename)

        # Initialize the database
        self.init_remote_db(remote_database_url)

    def init_cache(self, cache_dir : str | None = None, local_database_filename : str | None = None):
        self.cache_dir = cache_dir if cache_dir is not None else os.environ['HISPECDRP_CALIBRATION_CACHE']
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(os.path.join(self.cache_dir, 'calibrations'), exist_ok=True)
        os.makedirs(os.path.join(self.cache_dir, 'database'), exist_ok=True)
        local_db_filepath = os.path.join(self.cache_dir, 'database', local_database_filename)
        self.local_db = LocalCalibrationDB(db_path=local_db_filepath, orm_class=self.orm_class)

    def init_remote_db(self, remote_database_url : str | None = None):
        if remote_database_url is not None:
            remote_database_url = remote_database_url
        else:
            remote_database_url = os.environ.get('HISPECDRP_CALIBRATION_DATABASE_URL', self._DEFAULT_HISPECDRP_CALIBRATION_DATABASE_URL)
        if remote_database_url is not None:
            self.remote_db = RemoteCalibrationDB(url=remote_database_url)
        else:
            self.remote_db = None

    def _get_calibration(self, calibration, use_cached : bool = True) -> str:
        filepath_local = self.calibration_in_cache(calibration)
        if filepath_local is not None and use_cached:
            return filepath_local
        else:
            return self.download_calibration(calibration)
    
    def get_calibration(
        self,
        input : str | datamodels.HISPECDataModel,
        selector : CalibrationSelector,
        use_cached : bool = True,
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
        # NOTE: Change how this works if result = selector.select() is not a string, or implement in _get_calibration
        input = datamodels.open(input, meta_only=True)
        result = selector.select(input, self.local_db, self.orm_class, **kwargs)
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

    def register_local_calibration(
        self,
        calibration : datamodels.HISPECCalibrationModel | str,
        note : str | None = None
    ):
        """
        Register a calibration that is now stored in the appropriate calibrations directory and add to the local SQLLite DB.
        """
        output_dir = os.path.join(self.cache_dir, 'calibrations') + os.sep
        calibration.save(output_dir=output_dir)
        return self.local_db.add(calibration, note=note)
    
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