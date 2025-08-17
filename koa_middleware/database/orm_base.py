__all__ = ['CalibrationORM']

class CalibrationORM:
    """
    Lightweight Mixin base class for ORM objects.
    """

    def to_dict(self) -> dict:
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}

    def __repr__(self):
        fields = '\n'.join(f"  {col.name}: {getattr(self, col.name)}" for col in self.__table__.columns)
        return f"{self.__class__.__name__}\n{fields}"