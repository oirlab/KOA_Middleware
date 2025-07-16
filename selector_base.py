from hispecdrp import datamodels
from .database import CalibrationDB


__all__ = ['CalibrationSelector']


class CalibrationSelector:
    """
    Base class for calibration selectors. Can be used to select one or a group (e.g. bracketing etalon exposures) of calibrations.
    """
    def select(self, input : str | datamodels.HISPECDataModel, db : CalibrationDB, **kwargs):
        """
        Select the best calibration for the given input. This method should not be overriden by subclasses in most cases.

        Args:
            input (str | datamodels.HISPECDataModel): Input data file or model.
            db (CalibrationDB): Database session for querying.
            kwargs: Additional filtering parameters specific for the selector.

        Returns:
            Any: Selected calibration file or model.
        """
        input = datamodels.open(input, copy=False, meta_only=True)
        result = self._select(input, db, **kwargs)
        if result is None:
            result = self.select_fallback(input, db, **kwargs) # TODO: Implement fallback selection
        return result


    def _select(self, input : str | datamodels.HISPECDataModel, db : CalibrationDB, **kwargs):
        """
        Internal method to select the best calibration. Can be overridden by all subclasses. By default, it calls get_candidates followed by select_best.
        
        Args:
            input: Input science frame or its metadata.
            db: Database session for querying.
            kwargs: Additional filtering parameters
        
        Returns:
            Selected calibration file(s).
        """
        candidates = self.get_candidates(input, db, **kwargs)
        result = self.select_best(input, candidates, **kwargs)
        return result


    def get_candidates(self, input, db : CalibrationDB, **kwargs):
        """
        Get initial candidates.
        
        Args:
            input: Input data product.
            candidates: Initial query of candidates.
            kwargs: Additional filtering parameters.
        
        Returns:
            Candidate calibrations.
        """
        raise NotImplementedError(f"Class {self.__class__.__name__} must implement method get_candidates.")
    

    def select_best(self, input, candidates, **kwargs):
        """
        Select the best calibration(s) based on the candidates. By default, it returns the first candidate. This should be overriden by most subclasses.
        
        Args:
            input: Input data product.
            candidates: Candidate calibrations returned from self.get_candidates().
            kwargs: Additional filtering parameters.
        
        Returns:
            Selected calibration file(s).
        """
        return candidates[0] if candidates else None
    

    def select_fallback(self, input : str | datamodels.HISPECDataModel, db : CalibrationDB, **kwargs):
        """
        Select a fallback calibration if no suitable candidates are found. By default, it returns None. This should be overriden by most subclasses.
        
        Args:
            input: Input data product.
            db: Database session for querying.
            kwargs: Additional filtering parameters.
        
        Returns:
            Fallback calibration file(s).
        """
        return None