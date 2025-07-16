from .metadata_database import CalibrationDB
import glob
import os

__all__ = ['LocalCalibrationDB']

class LocalCalibrationDB(CalibrationDB):
    """
    Class to interface with local SQLite DB.
    """

    def __init__(self, db_path : str, orm_class : type):
        url = f"sqlite:///{db_path}"
        super().__init__(url, orm_class)

    def update_from_cache(self, cache_dir : str):
        """
        Update the local SQLite DB based on the cache dir.
        """
        calibrations = []
        cal_dir = f'{cache_dir}calibrations{os.sep}'
        cal_files = [os.path.abspath(f) for f in glob.glob(cal_dir + '*.fits')]
        for filepath in cal_files:
            calibration = self.orm_class.from_datamodel(filepath)
            calibrations.append(calibration)
        if calibrations:
            self.add(calibrations)