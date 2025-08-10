from .metadata_database import CalibrationDB

__all__ = ['LocalCalibrationDB']

class LocalCalibrationDB(CalibrationDB):
    """
    Class to interface with local SQLite DB.
    """

    def __init__(self, db_path : str, orm_class : type):
        url = f"sqlite:///{db_path}"
        super().__init__(url, orm_class)