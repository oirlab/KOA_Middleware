from .database import CalibrationDB


__all__ = ['CalibrationSelector']


class CalibrationSelector:
    """
    Base class for calibration selectors.

    This abstract base class defines the interface for selecting one or more calibrations
    based on input data and a database of available calibrations. Subclasses are expected
    to implement specific selection logic by overriding methods like `get_candidates`
    and `select_best`.

    A selector can be used to find a single best-fit calibration or a group of calibrations
    (e.g., bracketing etalon exposures).
    """
    def select(self, input, db : CalibrationDB, **kwargs):
        """
        Selects the best calibration for the given input data.

        This is the primary entry point for calibration selection. It calls the internal
        `_select` method and, if no result is found, attempts to use a fallback mechanism.
        Subclasses should generally not override this method, but rather `_select`,
        `get_candidates`, `select_best`, or `select_fallback`.

        Args:
            input: The input data file or model object for which a calibration is to be selected.
                   The exact type depends on the specific selector implementation.
            db (CalibrationDB): An instance of `CalibrationDB` (or a subclass) providing
                                access to the calibration database for querying.
            **kwargs: Additional filtering parameters that can be passed to the underlying
                      selection logic (e.g., `_select`, `get_candidates`, `select_best`).

        Returns:
            Any: The selected calibration file or model object. The type of the returned
                 object depends on the specific selector and the `CalibrationORM` used.
                 Returns `None` if no suitable calibration is found, even after fallback.
        """
        result = self._select(input, db, **kwargs)
        if result is None:
            result = self.select_fallback(input, db, **kwargs) # TODO: Implement fallback selection
        return result


    def _select(self, input, db : CalibrationDB, **kwargs):
        """
        Internal method to perform the core calibration selection logic.

        This method first retrieves a set of candidate calibrations using `get_candidates`
        and then selects the best one (or ones) from these candidates using `select_best`.
        Subclasses can override this method to implement custom selection workflows,
        but it's often more appropriate to override `get_candidates` or `select_best`.

        Args:
            input: The input data file or model object.
            db (CalibrationDB): The database instance for querying.
            **kwargs: Additional filtering parameters.

        Returns:
            Any: The selected calibration file(s) or model(s).
        """
        candidates = self.get_candidates(input, db, **kwargs)
        result = self.select_best(input, candidates, **kwargs)
        return result


    def get_candidates(self, input, db : CalibrationDB, **kwargs):
        """
        Abstract method to retrieve an initial set of candidate calibrations.

        Subclasses *must* implement this method to define how potential calibrations
        are identified from the database based on the input data and any additional criteria.

        Args:
            input: The input data file or model object.
            db (CalibrationDB): The database instance for querying.
            **kwargs: Additional filtering parameters.

        Returns:
            list: A list of candidate calibration objects (e.g., `CalibrationORM` instances).

        Raises:
            NotImplementedError: If the subclass does not implement this method.
        """
        raise NotImplementedError(f"Class {self.__class__.__name__} must implement method get_candidates.")
    

    def select_best(self, input, candidates, **kwargs):
        """
        Select the best calibration(s) based on the candidates. By default, it returns the first candidate. This should be overriden by most subclasses.
        
        Args:
            input: Input data file or model object to select a calibration for.
            candidates: Candidate calibrations returned from self.get_candidates().
            kwargs: Additional filtering parameters.
        
        Returns:
            Selected calibration file(s).
        """
        return candidates[0] if candidates else None
    

    def select_fallback(self, input, db : CalibrationDB, **kwargs):
        """
        Select a fallback calibration if no suitable candidates are found. By default, it returns None. This should be overriden by most subclasses.
        
        Args:
            input: Input data file or model object to select a calibration for.
            db: Database session for querying.
            kwargs: Additional filtering parameters.
        
        Returns:
            Fallback calibration file(s).
        """
        return None