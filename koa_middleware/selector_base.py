from .database import LocalCalibrationDB

import re


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
        - ``select_best``: Choose the best calibration(s) from the list of candidates. By default, this method returns the first candidate if it's a list, or the input as is otherwise.
        - ``_select``: Customize the overall selection workflow by combining candidate retrieval and best selection. This does not call ``select_fallback``; that is handled in ``select``.
    """

    def select(self, input, db : LocalCalibrationDB, **kwargs) -> dict | None:
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
        **kwargs
            Additional filtering parameters as necessary passed to ``self._select``.

        Returns
        -------
        result : dict | None
            The selected calibration metadata dictionary.
            Returns `None` if no suitable calibration is found, even after fallback.
        """
        result = self._select(input, db, **kwargs)
        if result is None:
            result = self.select_fallback(input, db, **kwargs)
        return result

    def _select(self, input, db : LocalCalibrationDB, **kwargs) -> dict | None:
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
        **kwargs
            Additional filtering parameters as necessary. Passed to both ``get_candidates`` and ``select_best``.

        Returns
        -------
        result : dict | None
            The selected calibration metadata dictionary.
        """
        candidates = self.get_candidates(input, db, **kwargs)
        result = self.select_best(input, candidates, **kwargs)
        return result

    def get_candidates(self, input, db : LocalCalibrationDB, **kwargs) -> list[dict]:
        """
        Primary method called to retrieve an initial set of candidate calibrations from the local DB.
        Subclasses *must* implement this method.

        Parameters
        ----------
        input : dict
            The input object for which a calibration is to be selected.
            The exact type depends on the specific selector implementation.
        db : LocalCalibrationDB
            The database instance for querying.
        **kwargs
            Additional filtering parameters as necessary.

        Returns
        -------
        list[dict]
            A list of candidate calibration metadata dictionaries.
        """
        raise NotImplementedError(f"Class {self.__class__.__name__} must implement method get_candidates.")
    
    def select_best(self, input, candidates : list[dict], **kwargs) -> dict | None:
        """
        Select the best calibration(s) based on the candidates.
        By default, it returns the first candidate.
        This can optionally be overridden by subclasses.
        
        Parameters
        ----------
        input
            The input object for which a calibration is to be selected.
            The exact type depends on the specific selector implementation.
        candidates : list[dict]
            Candidate calibrations returned from `get_candidates()`.
        **kwargs
            Additional filtering parameters as necessary.
        
        Returns
        -------
        dict | None
            Selected calibration metadata dictionary, or None if no candidates available.
        """
        if isinstance(candidates, list):
            return candidates[0] if candidates else None
        return candidates
    
    def select_fallback(self, input, db : LocalCalibrationDB, **kwargs) -> dict | None:
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
        **kwargs
            Additional filtering parameters as necessary.
        
        Returns
        -------
        dict | None
            Fallback calibration metadata dictionary, or None if not available.
        """
        return None
    
    import re

    @staticmethod
    def get_query_params(input: dict, sql: str, strict: bool = True) -> dict:
        """
        Extract only the SQL parameters used in the query from the input dict.

        Parameters
        ----------
        input : dict
            Input metadata dictionary.
        sql : str
            SQL query string to determine which parameters are needed.
        strict : bool, optional
            If True (default), raise an error if a placeholder in the SQL is missing from input.

        Returns
        -------
        dict
            Dictionary of query parameters for database querying.
        """
        placeholders = set(re.findall(r":([a-zA-Z_]\w*)", sql))
        params_query = {k: v for k, v in input.items() if k in placeholders}

        if strict:
            missing = placeholders - params_query.keys()
            if missing:
                raise ValueError(f"Missing query parameters: {missing}")

        return params_query
