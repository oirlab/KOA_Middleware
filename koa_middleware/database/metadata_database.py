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
    """
    A generic utility class for interfacing with calibration databases.

    This class provides a foundational layer for database operations, supporting both
    local SQLite and remote PostgreSQL databases. It manages SQLAlchemy engine creation,
    session management, and common CRUD (Create, Read, Update, Delete) operations
    for `CalibrationORM` objects.

    It is designed to be extended by specific database implementations (e.g., `LocalCalibrationDB`,
    `RemoteCalibrationDB`).

    Attributes:
        engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine connected to the database.
        Session (sqlalchemy.orm.session.sessionmaker): A session factory for creating new database sessions.
        orm_class (type[CalibrationORM]): The SQLAlchemy ORM class used to define the database schema.
    """

    def __init__(self, url : str, orm_class : type[CalibrationORM]):
        """
        Initializes a new instance of the CalibrationDB.

        This constructor sets up the SQLAlchemy engine and session factory for the database
        specified by the URL. It also ensures that the database schema (tables) defined by
        the `orm_class` are created if they do not already exist.

        Args:
            url (str): The database connection URL. This can be a SQLite URL (e.g., `sqlite:///path/to/db.db`)
                       or a PostgreSQL URL (e.g., `postgresql://user:password@host:port/database`).
            orm_class (type[CalibrationORM]): The SQLAlchemy ORM class that defines the database schema
                                              and the structure of the calibration data to be stored.

        Example:
            >>> from sqlalchemy.ext.declarative import declarative_base
            >>> from sqlalchemy import Column, Integer, String
            >>> Base = declarative_base()
            >>> class MyCalibrationORM(Base):
            ...     __tablename__ = 'my_calibrations'
            ...     id = Column(Integer, primary_key=True)
            ...     name = Column(String)
            >>> db = CalibrationDB('sqlite:///./test.db', MyCalibrationORM)
        """
        self.engine = self.get_engine(url=url)
        orm_class.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.orm_class = orm_class

    def get_engine(self, url : str, echo : bool = True):
        """
        Creates and returns a SQLAlchemy engine for the given database URL.

        Args:
            url (str): The database connection URL.
            echo (bool): If `True`, SQLAlchemy will log all SQL statements to stdout.
                         Defaults to `True`.

        Returns:
            sqlalchemy.engine.base.Engine: The created SQLAlchemy engine.
        """
        return create_engine(url, echo=echo)

    def close(self):
        """
        Disposes of the SQLAlchemy engine, closing all connections in the connection pool.

        It is important to call this method when the database object is no longer needed
        to release resources.
        """
        self.engine.dispose()

    @contextmanager
    def session_manager(self, external_session: Session | None = None):
        """
        Provides a context manager for managing SQLAlchemy database sessions.

        This context manager ensures that sessions are properly created, committed,
        rolled back on errors, and closed. It can either create its own session
        or use an externally provided session.

        Args:
            external_session (Session | None): An optional existing SQLAlchemy session
                                               to use. If `None`, a new session will be created.

        Yields:
            Session: The SQLAlchemy session to be used within the context.

        Raises:
            SQLAlchemyError: Any exception raised during database operations within the context
                             will cause a rollback and be re-raised.

        Example:
            >>> # Assuming 'db' is an instance of CalibrationDB
            >>> with db.session_manager() as session:
            ...     # Perform database operations using 'session'
            ...     pass
            >>> # Or with an external session:
            >>> # my_session = db.Session()
            >>> # with db.session_manager(my_session) as session:
            >>> #     pass
            >>> # my_session.close()
        """
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
        Retrieves the most recent `LAST_UPDATED` timestamp from the database.

        This method queries the `orm_class` table to find the maximum value
        in the `last_updated` column.

        Args:
            session (Session | None): An optional SQLAlchemy session to use.
                                      If `None`, a new session will be created and managed
                                      by the `session_manager` context.

        Returns:
            str: The most recent `LAST_UPDATED` timestamp as an ISO formatted string.
                 Returns `None` if the table is empty or no `last_updated` values exist.
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
    ) -> list[CalibrationORM]:
        """
        Queries the database for calibration entries based on various criteria.

        This method allows filtering calibrations by type and observation datetime range.

        Args:
            session (Session | None): An optional SQLAlchemy session to use.
                                      If `None`, a new session will be created and managed.
            cal_type (str | None): An optional calibration type string to filter the results.
                                   If `None`, calibrations of all types are included.
            date_time_start (str | None): The start datetime (inclusive) for filtering calibrations,
                                          in ISO format (e.g., 'YYYY-MM-DDTHH:MM:SS.sss').
                                          If `None`, defaults to the earliest possible datetime.
            date_time_end (str | None): The end datetime (inclusive) for filtering calibrations,
                                        in ISO format (e.g., 'YYYY-MM-DDTHH:MM:SS.sss').
                                        If `None`, defaults to the latest possible datetime.
            fetch (str): Specifies how many results to fetch:
                         - `'all'`: Fetches all matching results (default).
                         - `'first'`: Fetches only the first matching result.

        Returns:
            list[CalibrationORM]: A list of `CalibrationORM` objects that match the query criteria.
                                  If `fetch` is `'first'`, the list will contain at most one element.

        Example:
            >>> # Assuming 'db' is an instance of CalibrationDB
            >>> # all_cals = db.query()
            >>> # type_filtered_cals = db.query(cal_type='BIAS')
            >>> # time_filtered_cals = db.query(date_time_start='2023-01-01T00:00:00', date_time_end='2023-01-31T23:59:59')
            >>> # first_cal = db.query(fetch='first')
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
        Adds one or more calibration objects to the database.

        This method uses `session.merge()` to add or update calibration entries,
        allowing for idempotent operations (i.e., adding an existing entry will update it).

        Args:
            calibration (CalibrationORM | list[CalibrationORM]): A single `CalibrationORM` object
                                                                 or a list of `CalibrationORM` objects to add.
            session (Session | None): An optional SQLAlchemy session to use.
                                      If `None`, a new session will be created and managed.
            commit (bool): If `True`, the transaction will be committed after adding the calibration(s).
                           Defaults to `True`. Set to `False` if you intend to perform more operations
                           within the same session before committing.

        Example:
            >>> # Assuming 'db' is an instance of CalibrationDB and 'new_cal' is a CalibrationORM object
            >>> # db.add(new_cal)
            >>> # db.add([cal1, cal2], commit=False) # Add multiple without immediate commit
        """
        if not isinstance(calibration, list):
            calibration = [calibration]
        with self.session_manager(session) as session:
            for item in calibration:
                session.merge(item)
            if commit:
                session.commit()

    def query_by_id(self, calibration_id : str, session : Session | None = None) -> list[CalibrationORM]:
        """
        Queries the database for calibration entries by their unique ID.

        Args:
            calibration_id (str): The unique identifier of the calibration to query.
            session (Session | None): An optional SQLAlchemy session to use.
                                      If `None`, a new session will be created and managed.

        Returns:
            list[CalibrationORM]: A list of `CalibrationORM` objects that match the given ID.
                                  Returns an empty list if no matching calibration is found.

        Example:
            >>> # Assuming 'db' is an instance of CalibrationDB
            >>> # cal = db.query_by_id('some_calibration_id')
            >>> # if cal:
            >>> #     print(f"Found calibration with ID: {cal[0].id}")
        """

        with self.session_manager(session) as session:
            return session.query(self.orm_class).filter_by(id=calibration_id).all()