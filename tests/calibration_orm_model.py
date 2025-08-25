from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, Float, Boolean
from sqlalchemy.orm import declarative_base
import uuid

from koa_middleware.database.orm_base import CalibrationORM

__all__ = ['CalibrationTestORM']

_Base = declarative_base()

class CalibrationTestORM(CalibrationORM, _Base):
    __tablename__ = "test_calibrations"

    id: Mapped[uuid.UUID] = mapped_column(String(36), primary_key=True)
    filename: Mapped[str] = mapped_column(String, nullable=False)
    master: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    cal_type: Mapped[str] = mapped_column(String, nullable=False)
    instera: Mapped[str] = mapped_column(String, nullable=False)
    spectrograph: Mapped[str] = mapped_column(String, nullable=False)
    mjd_start: Mapped[float] = mapped_column(Float, nullable=False)
    last_updated: Mapped[str] = mapped_column(String, nullable=False)
