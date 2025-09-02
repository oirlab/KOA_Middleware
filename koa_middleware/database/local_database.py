from .metadata_database import CalibrationDB

__all__ = ['LocalCalibrationDB']

class LocalCalibrationDB(CalibrationDB):
    """
    A class to interface with a local SQLite database for calibration data.

    This class extends `CalibrationDB` and provides methods for interacting with a SQLite database
    stored locally. It is designed to manage calibration metadata and facilitate data retrieval
    and storage operations.

    Attributes:
        db_path (str): The file path to the SQLite database.
        orm_class (type): The SQLAlchemy ORM class used for database interactions.
    """

    def __init__(self, db_path : str, orm_class : type):
        """
        Initializes the LocalCalibrationDB instance.

        Args:
            db_path (str): The absolute path to the SQLite database file (e.g., '/path/to/my_calibration.db').
            orm_class (type): The SQLAlchemy ORM class that defines the table structure for the calibration data.
                              This class should inherit from `sqlalchemy.ext.declarative.declarative_base()`.

        Example:
            >>> from koa_middleware.database.orm_base import Base
            >>> from sqlalchemy import Column, Integer, String
            >>> class MyCalibration(Base):
            ...     __tablename__ = 'my_calibrations'
            ...     id = Column(Integer, primary_key=True)
            ...     name = Column(String)
            >>> db = LocalCalibrationDB('/tmp/test_cal.db', MyCalibration)
        """
        url = f"sqlite:///{db_path}"
        super().__init__(url, orm_class)