import abc


class AbstractCalibrationModel(abc.ABC):
    """
    Structural interface for DataModel-like objects that can be written to disk
    and converted into a calibration database record.

    Any class instance that wishes to be treated as an AbstractCalibrationModel
    must implement the methods defined here.
    """

    @classmethod
    def __subclasshook__(cls, c_):
        """
        Pseudo subclass check based on required methods.
        """
        if cls is AbstractCalibrationModel:
            mro = c_.__mro__
            if (
                any(hasattr(CC, "save") for CC in mro)
                and any(hasattr(CC, "to_record") for CC in mro)
            ):
                return True
        return False

    @abc.abstractmethod
    def save(
        self, *args,
        output_path : str | None = None,
        output_dir: str | None = None,
        **kwargs
    ) -> str:
        """
        Save the data model to disk.

        Parameters
        ----------
        output_path : str, optional
            Full file path to save the file to. If None, saves to the current directory.
        output_dir : str, optional
            Directory to save the file to. Ignored if `output_path` is provided.

        Returns
        -------
        str
            The full file path where the model was saved.
        """

    @abc.abstractmethod
    def to_record(self, *args, **kwargs) -> dict:
        """
        Convert this model into a calibration database record.

        Returns
        -------
        dict
            A dictionary representing the calibration database record.
        """
