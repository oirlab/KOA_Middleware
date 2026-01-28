# from koa_middleware.database.metadata_database import CalibrationDB
# from sqlalchemy import func

# from koa_middleware.selector_base import CalibrationSelector

# __all__ = [
#     'TestDarkSelector',
# ]

# class TestDarkSelector(CalibrationSelector):

#     def get_candidates(self, meta : dict, db : CalibrationDB):
#         with db.session_manager() as session:
#             # Simplified for a generic test instrument
#             return session.query(db.orm_class).filter(
#                 db.orm_class.cal_type == 'dark',
#                 db.orm_class.instrument_era == meta['instrument_era'],
#                 db.orm_class.spectrograph == meta['spectrograph'],
#                 db.orm_class.master_cal == True
#             ).order_by(func.abs(db.orm_class.mjd_start - meta['mjd_start'])).all()

from koa_middleware.selector_base import CalibrationSelector, LocalCalibrationDB

__all__ = [
    'TestDarkSelector',
]

class TestDarkSelector(CalibrationSelector):

    def get_candidates(self, meta: dict, db : LocalCalibrationDB):
        """
        Return candidate calibration entries for testing using sqlite-utils.
        """
        sql = """
            cal_type = :cal_type
            AND instrument_era = :era
            AND spectrograph = :spec
            AND master_cal = 1
        """
        params = {
            "cal_type": "dark",
            "era": meta["instrument_era"],
            "spec": meta["spectrograph"],
        }

        # Fetch all matching rows
        rows = list(db.table.rows_where(sql, params))

        # Sort by absolute difference in mjd_start
        rows.sort(key=lambda r: abs(r["mjd_start"] - meta["mjd_start"]))

        return rows
