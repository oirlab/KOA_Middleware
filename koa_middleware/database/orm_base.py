__all__ = ['CalibrationORM']

class CalibrationORM:
    """
    A lightweight mixin base class for SQLAlchemy ORM objects representing calibration data.

    This class provides common utility methods that can be inherited by SQLAlchemy declarative
    base classes to facilitate conversion to dictionary format and a readable string representation.

    Classes inheriting from `CalibrationORM` are expected to be SQLAlchemy models
    with defined table columns.
    """

    def to_dict(self) -> dict:
        """
        Converts the ORM object's column data into a dictionary.

        The keys of the dictionary will be the column names, and the values will be
        the corresponding attribute values of the ORM object.

        Returns:
            dict: A dictionary representation of the ORM object's data.
        """
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}

    def __repr__(self):
        fields = '\n'.join(f"  {col.name}: {getattr(self, col.name)}" for col in self.__table__.columns)
        return f"{self.__class__.__name__}\n{fields}"