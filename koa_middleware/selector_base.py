from .database import LocalCalibrationDB
import os

import logging
logger = logging.getLogger(__name__)

__all__ = ['CalibrationSelector']


class CalibrationSelector:
    """
    Base class for calibration selectors.

    This abstract base class defines the interface for selecting one or more calibrations
    based on input data and a database of available calibrations. Subclasses are expected
    to implement specific selection logic by overriding methods like ``get_candidates``
    and ``select_best``.

    Required methods to implement:
        - ``get_candidates``: Retrieve a list of candidate calibrations from the database. Optionally, this method can also return a single candidate if desired.

    Optional methods to override:
        - ``select_fallback``:  Provide a fallback selection mechanism if no candidates are found.
        - ``select_best``: Choose the best calibration(s) from the list of candidates. See method for default behavior.
        - ``_select``: Customize the overall selection workflow by combining candidate retrieval and best selection. This does not call ``select_fallback``; that is handled in ``select``.
    """

    def __init__(
        self, *args,
        origin: str | None = None,
        **kwargs
    ):
        if origin is None:
            origin = os.getenv("KOA_CALIBRATION_ORIGIN", "ANY")
        self.origin = origin.upper()

        for kwarg in kwargs:
            setattr(self, kwarg, kwargs[kwarg])

    def select(self, input, db : LocalCalibrationDB) -> dict | None:
        """
        Selects the best calibration for the given input data.

        This is the primary entry point for calibration selection. It does the following:

        1. Calls the internal ``_select`` method to perform the main selection logic, which calls:
              - ``get_candidates`` to retrieve candidate calibrations.
              - ``select_best`` to choose the best calibration from the candidates.

        2. If no suitable calibration is found, it calls ``select_fallback`` to attempt
           a fallback selection.

        Parameters
        ----------
        input
            The input object for which a calibration is to be selected.
            The exact type depends on the specific selector implementation.
        db : LocalCalibrationDB
            The database instance for querying. 

        Returns
        -------
        result : dict | None
            The selected calibration metadata dictionary.
            Returns `None` if no suitable calibration is found, even after fallback.
        """
        result = self._select(input, db)
        if result is None:
            result = self.select_fallback(input, db)
        return result

    def _select(self, input, db : LocalCalibrationDB) -> dict | None:
        """
        Internal method to perform the core calibration selection logic.

        This method first retrieves a set of candidate calibrations using `get_candidates`
        and then selects the best one (or ones) from these candidates using `select_best`.
        Subclasses can override this method to implement custom selection workflows,
        but it's often more appropriate to override `get_candidates` or `select_best`.

        Parameters
        ----------
        input
            The input object for which a calibration is to be selected.
            The exact type depends on the specific selector implementation.
        db : LocalCalibrationDB
            The database instance for querying. 

        Returns
        -------
        result : dict | None
            The selected calibration metadata dictionary.
        """
        candidates = self.get_candidates(input, db)
        result = self.select_best(input, candidates)
        return result

    def get_candidates(self, input, db : LocalCalibrationDB) -> list[dict] | dict:
        """
        Primary method called to retrieve an initial set of candidate calibrations from the local DB.
        Subclasses *must* implement this method.

        Parameters
        ----------
        input
            The input object for which a calibration is to be selected.
            The exact type depends on the specific selector implementation.
        db : LocalCalibrationDB
            The database instance for querying.

        Returns
        -------
        list[dict] | dict
            A list of candidate calibration metadata records, or a single candidate records.
        """
        raise NotImplementedError(f"Class {self.__class__.__name__} must implement method get_candidates.")
    
    def select_best(self, input, candidates : list[dict] | dict) -> dict | None:
        """
        Select the best calibration(s) based on the candidates.
        The default implementation simply returns the first candidate if candidates is a list, or the candidate itself if it's a dict.
        
        Parameters
        ----------
        input
            The input object for which a calibration is to be selected.
            The exact type depends on the specific selector implementation.
        candidates : list[dict] | dict
            Candidate calibrations returned from `get_candidates()`.
        
        Returns
        -------
        dict | None
            Selected calibration metadata record, or None if no candidates available.
        """
        if isinstance(candidates, dict):
            return candidates
        return candidates[0] if candidates else None
    
    def select_fallback(self, input, db : LocalCalibrationDB) -> dict | None:
        """
        Select a fallback calibration if no suitable candidates are found.
        Default implementation returns ``None``.
        This must be overridden by subclasses if needed.
        
        Parameters
        ----------
        input
            The input object for which a fallback calibration is to be selected.
            The exact type depends on the specific selector implementation.
        db : LocalCalibrationDB
            Database instance for querying.
        
        Returns
        -------
        dict | None
            Fallback calibration metadata record, or None if not available.
        """
        return None
    
    def __repr__(self):
        attrs = ', '.join([f'{k}={v}' for k, v in self.__dict__.items()])
        return f"{self.__class__.__name__}({attrs})"
    