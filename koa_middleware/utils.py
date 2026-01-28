import datetime
import re
import os
    
def generate_koa_filepath(
    instrument : str,
    instrument_prefix : str,
    data_level : str,
    date_obs : str,
    utc_obs : str
) -> str:
    """
    Generates the filepath on KOA.
    
    Format: "/{instrument}/{year}/{date_obs}/{data_level}/{koa_id}" where:
    
    - `instrument` is the instrument name.
    - `year` is the year of the observation in YYYY format.
    - `date_obs` is the observation date in 'YYYYMMDD' format.
    - `data_level` is the data level (e.g., 'L1').
    - `koa_id` is the KOA ID (see `generate_koa_id`).

    Parameters
    ----------
    instrument : str
        The instrument name.
    instrument_prefix : str
        The two-letter prefix for the instrument.
    data_level : str
        The data level (e.g., 'L1').
    date_obs : str
        The observation date in 'YYYYMMDD' format.
    utc_obs : str
        The UTC observation time in 'HH:MM:SS.sss' format.

    Returns
    -------
    str
        The KOA filepath for the given input parameters.

    Example
    -------
    >>> generate_koa_filepath('HISPEC', 'HR', 'L0', '20240924', '12:34:56.78')
    '/HISPEC/2024/20240924/L0/HR.20240924.45296.78.fits'
    """
    year = date_obs[0:4]
    koa_id = generate_koa_id(instrument_prefix, date_obs, utc_obs)
    return f"/{instrument.upper()}/{year}/{date_obs}/{data_level}/{koa_id}"

def generate_koa_id(instrument_prefix : str, date_obs : str = None, utc_obs : str = None, ext : str = 'fits') -> str:
    """
    Generates the KOA ID for a calibration or data file.
    
    Format: "{instrument_prefix}.{date_obs}.{seconds}.{ext}" where:
    
    - `instrument_prefix` is the two-letter prefix for the instrument.
    - `date_obs` is the observation date in 'YYYYMMDD' format.
    - `seconds` is the total number of seconds since midnight UTC, formatted as 'SSSSS.ss'.
    - `ext` is the file extension.

    Parameters
    ----------
    instrument_prefix : str
        The two-letter prefix for the instrument.
    date_obs : str, optional
        The observation date in 'YYYYMMDD' format.
    utc_obs : str, optional
        The UTC observation time in 'HH:MM:SS.sss' format.
    ext : str, optional
        The file extension. Default is 'fits'.

    Returns
    -------
    str
        The KOA ID string.

    Example
    -------
    >>> generate_koa_id('HR', '20240924', '12:34:56.78')
    'HR.20240924.45296.78.fits'
    """
    utc = datetime.datetime.strptime(utc_obs, '%H:%M:%S.%f')
    total_seconds = utc.hour * 3600 + utc.minute * 60 + utc.second + utc.microsecond / 1e6
    seconds = f"{total_seconds:08.2f}"
    return f"{instrument_prefix}.{date_obs}.{seconds}.{ext}"

_uuid_regex = re.compile(
    r'^[a-f0-9]{8}-'
    r'[a-f0-9]{4}-'
    r'4[a-f0-9]{3}-'
    r'[89ab][a-f0-9]{3}-'
    r'[a-f0-9]{12}\Z',
    re.I
)

def is_valid_uuid(value: str) -> bool:
    """
    Checks if a given string is a valid UUID v4.

    Parameters
    ----------
    value : str
        The string to check.

    Returns
    -------
    bool
        True if the string is a valid UUID v4, False otherwise.
    """
    return bool(_uuid_regex.match(value))


def get_env_var_bool(name : str, default : bool | None = None) -> bool | None:
    """
    Return the boolean value of an environment variable.

    Parameters
    ----------
    name : str
        The name of the environment variable.
    default : bool | None, optional
        The default value to return if the environment variable is not set. Default is None.

    Returns
    -------
    bool | None
        The boolean value of the environment variable, or the default if not set.
    """
    val = os.environ.get(name)
    if val is None:
        return default
    return val.lower() in {"1", "true", "yes", "on"}