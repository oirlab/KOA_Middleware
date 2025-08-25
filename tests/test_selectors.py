from koa_middleware.database.metadata_database import CalibrationDB
from sqlalchemy import func

from koa_middleware.selector_base import CalibrationSelector

__all__ = [
    'TestDarkSelector',
]

class TestDarkSelector(CalibrationSelector):

    def get_candidates(self, meta : dict, db : CalibrationDB):
        with db.session_manager() as session:
            # Simplified for a generic test instrument
            return session.query(db.orm_class).filter(
                db.orm_class.cal_type == 'dark',
                db.orm_class.instera == meta['instrument.era'],
                db.orm_class.spectrograph == meta['instrument.spectrograph'],
                db.orm_class.master == True
            ).order_by(func.abs(db.orm_class.mjd_start - meta['exposure.mjd_start'])).all()
