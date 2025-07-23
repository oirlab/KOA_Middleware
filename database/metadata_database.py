from contextlib import contextmanager
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import datetime
from .orm_base import CalibrationORM

__all__ = ['CalibrationDB']


class CalibrationDB:
    """Generic utility class to interface with a local SQLite DB or remote PostgreSQL DB."""

    def __init__(self, url : str, orm_class : type):
        self.engine = self.get_engine(url=url)
        orm_class.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.orm_class = orm_class

    def get_engine(self, url : str, echo : bool = True):
        """Connect to DB"""
        return create_engine(url, echo=echo)

    def close(self):
        """Close the database engine."""
        self.engine.dispose()

    @contextmanager
    def session_manager(self, external_session: Session | None = None):
        own_session = external_session is None
        session = self.Session() if own_session else external_session
        try:
            yield session
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            if own_session:
                session.close()
    
    def get_last_updated(self, session : Session | None = None) -> str:
        """
        Get most recent LAST_UPDATED timestamp from the database.
        """
        with self.session_manager(session) as session:
            last_updated = session.query(func.max(self.orm_class.last_updated)).scalar()
        return last_updated

    def query(
        self,
        session : Session | None = None,
        cal_type : str | None = None,
        date_time_start : str | None = None,
        date_time_end : str | None = None,
        fetch : str = 'all',
    ) -> list:
        """
        Higher level query method to retrieve calibrations based on a specified calibration type and datetime start/end.
        The utility of this method will be revisited as the DRP is developed.
        """
        if date_time_start is None:
            date_time_start = datetime.datetime.min.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        if date_time_end is None:
            date_time_end = datetime.datetime.max.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        with self.session_manager(session) as session:
            _query = session.query(self.orm_class).filter(
                self.orm_class.datetime_obs >= date_time_start,
                self.orm_class.datetime_obs <= date_time_end
            )
            if cal_type is not None:
                _query = _query.filter(self.orm_class.cal_type == cal_type)
            if fetch == 'all':
                result = _query.all()
            elif fetch == 'first':
                result = _query.first()
        return result

    def add(
        self,
        calibration : CalibrationORM | list[CalibrationORM],
        session : Session | None = None,
        commit : bool = True,
    ):
        """
        Add one or many calibrations to the database.
        """
        if not isinstance(calibration, list):
            calibration = [calibration]
        with self.session_manager(session) as session:
            for item in calibration:
                session.merge(item)
            if commit:
                session.commit()



