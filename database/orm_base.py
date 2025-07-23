__all__ = ['CalibrationORM']

class CalibrationORM:
    """
    Lightweight Mixin base class for ORM objects.
    """

    def to_dict(self) -> dict:
        return {col.name: getattr(self, col.name) for col in self.__table__.columns}

    @classmethod
    def from_dict(cls, data: dict) -> 'CalibrationORM':
        return cls(**data)

    def __repr__(self):
        fields = '\n'.join(f"  {col.name}: {getattr(self, col.name)}" for col in self.__table__.columns)
        return f"{self.__class__.__name__}\n{fields}"

    @classmethod
    def from_datamodel(cls, *args, **kwargs) -> 'CalibrationORM':
        raise NotImplementedError(f"Method from_datamodel not implemented by class {cls}.")
    
    def __init_subclass__(cls):
        super().__init_subclass__()
        name = cls.__name__
        suffix = "CalibrationORM"
        if suffix in name:
            cls.instrument = name[:name.find(suffix)].lower()
        else:
            cls.instrument = None