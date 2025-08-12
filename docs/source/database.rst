====================
Calibration Database
====================

Calibration metadata is stored in an SQL based database (DB) to allow for faster parsing of metadata to select an appropriate calibration file. The DB is implemented in `SQLAlchemy ORM <https://www.sqlalchemy.org/>`_ and supports SQLite and PostgreSQL.

Object Relational Mapping (ORM)
-------------------------------

The database columns are declared in a standard Python class with additional methods for initialization. An ORM must inherit from *both*  :py:class:`~koa_middleware.database.orm_base.CalibrationORM` and the SQLAlchemy `delcarative base <https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/basic_use.html>`_. Below is the minimum specification for a new ORM:

*Note the minimum requirement will likely change as these protocols are further developed.*

Below is a simple example for creating a new ORM for an instrument called `MyInstrument` (e.g. HISPEC).

.. code-block:: python

   from sqlalchemy.orm import Mapped
   from sqlalchemy.orm import mapped_column
   from sqlalchemy import String, Float, Boolean
   from sqlalchemy.orm import declarative_base
   import uuid
   from koa_middleware import CalibrationORM
   _Base = declarative_base()

   class MyCalibrationORM(CalibrationORM, _Base):
      # Can be any valid table name
      __tablename__ = "MyInstrument"

      # Unique identifier for the calibration
      id: Mapped[uuid.UUID] = mapped_column(String(36), primary_key=True)

      # Unique identifier for the calibration
      koa_filepath: Mapped[str] = mapped_column(String(255), nullable=True)

      # When the calibration was inserted into the DB
      last_updated: Mapped[str] = mapped_column(String(50), nullable=False)


Any number of other fields can be added to the ORM class.


CalibrationDB
-------------

The base class :py:class:`~koa_middleware.database.metadata_database.CalibrationDB` wraps the SQLAlchemy interface (i.e. the SQLAlchemy engine and session objects). Two subclasses are provided for managing the local and remote databases, respectively.

Key methods:

- :py:meth:`~koa_middleware.database.metadata_database.CalibrationDB.add`

   Add one or many ``CalibrationORM``'s to the database.

- :py:meth:`~koa_middleware.database.metadata_database.CalibrationDB.query`

   Higher level query method to retrieve ``CalibrationORM``'s based on a specified calibration type and datetime start/end. The utility of this method will be revisited as the DRP is developed.

- :py:meth:`~koa_middleware.database.metadata_database.CalibrationDB.get_last_updated`

   Retrieve the most recent last_updated timestamp.

- :py:meth:`~koa_middleware.database.metadata_database.CalibrationDB.query_by_id`

   Retrieve a calibration by its ID.


Remote Database
---------------

:py:class:`~koa_middleware.database.remote_database.RemoteCalibrationDB` is a subclass of :py:class:`~koa_middleware.database.metadata_database.CalibrationDB` for remote databases (e.g., PostgreSQL).


Local Database
--------------

:py:class:`~koa_middleware.database.local_database.LocalCalibrationDB` is a subclass of :py:class:`~koa_middleware.database.metadata_database.CalibrationDB` for local SQLite databases.