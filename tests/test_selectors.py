from koa_middleware.selector_base import CalibrationSelector, LocalCalibrationDB

__all__ = [
    '_TestDarkSelector',
]

class _TestDarkSelector(CalibrationSelector):

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
            "mjd_start": meta["mjd_start"],
        }

        # Fetch all matching rows
        rows = list(db.table.rows_where(
            sql, params,
            order_by="ABS(mjd_start - :mjd_start)"
        ))

        return rows
