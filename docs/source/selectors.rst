=====================
Calibration Selectors
=====================

Calibration selectors are Python classes that specify the logic to select a calibration file from the local calibration cache given an input datamodel from a DRP. All calibration selectors inherit from :py:class:`~koa_middleware.selector_base.CalibrationSelector`.


Define New Selector
-------------------

Any selector can implement the following three methods to fully specify its behavior:

.. code-block:: python

    def get_candidates(self, input, db : CalibrationDB) -> list[CalibrationORM]:
        """
        Get a list of candidate calibrations from the database.
        This method is mandatory for any Selector and will perform an SQL query.
        """
        pass

    def select_best(self, input, candidates : list[CalibrationORM]) -> CalibrationORM:
        """
        Select the best calibration from a list of candidates.
        By default, the first candidate is returned (i.e., candidates[0]).
        """
        return candidates[0] if len(candidates) > 0 else None # This is redundant with the default behavior of the base class.

    def select_fallback(self, input, db : CalibrationDB) -> CalibrationORM:
        """
        Select a fallback calibration if no candidates are found (e.g. if len(candidates) == 0).
        NOTE: TBD how this is implemented and utilized and is only called if no valid calibration is found.
        """
        pass

.. code-block:: python
    :caption: Example of a selector class for a dark calibration for HISPEC

    from koa_middleware import CalibrationSelector
    # Other imports ...

    class DarkSelector(CalibrationSelector):

        def get_candidates(self, meta : dict, db : CalibrationDB):
            with db.session_manager() as session:
                if meta['instrument.name'].lower() == 'hispec':
                    return session.query(db.orm_class).filter(
                        db.orm_class.cal_type == 'dark',
                        db.orm_class.instera == meta['instrument.era'],
                        db.orm_class.spectrograph == meta['instrument.spectrograph'],
                        db.orm_class.master == True
                    ).order_by(func.abs(db.orm_class.mjd_start - meta['exposure.mjd_start'])).all()
                elif meta['instrument.name'].lower() == 'parvi':
                    return session.query(db.orm_class).filter(
                        db.orm_class.cal_type == 'dark',
                        db.orm_class.instera == meta['instrument.era'],
                    ).order_by(func.abs(db.orm_class.mjd_start - meta['exposure.mjd_start'])).all()