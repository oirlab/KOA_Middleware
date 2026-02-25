from datetime import datetime, timezone
import re
import os
import hashlib

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

def generate_md5_file(filepath: str) -> str:
    """
    Generate the MD5 checksum of a FITS file.

    Parameters
    ----------
    filepath : str
        The path to the file for which to compute the MD5 checksum.

    Returns
    -------
    str
        The MD5 checksum of the file.
    """
    chunk_size = 1024 * 1024
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def get_koa_id_timestamp_from_datetime(dt : str):
    """
    Get the KOA ID from a datetime string.

    Parameters
    ----------
    dt : str
        The datetime string in ISO format.

    Returns
    -------
    koa_id : str
        The KOA ID timestamp in the format 'YYYYMMDD.SSSSS.ss'.
    """
    utc = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%f')
    total_seconds = utc.hour * 3600 + utc.minute * 60 + utc.second + utc.microsecond / 1e6
    seconds = f"{total_seconds:08.2f}"
    date = utc.strftime('%Y%m%d')
    return f"{date}.{seconds}"

def generate_koa_filehandle(
    instrument_name : str,
    datetime_obs : str,
    koa_id : str
) -> str:
    """
    Generate a KOA filehandle.
    
    Format:
    
        ``/{instrument_name}/YYYY/YYMMDD/{koa_id}``
        
    where:
        - `instrument_name` is the instrument name.
        - `YYYY` is the 4-digit year of the observation.
        - `YYMMDD` is the date of the observation in year-month-day format.
        - `koa_id` is the KOA ID (same as the filename for HISPEC and PARVI).

    Parameters
    ----------
    instrument_name : str
        The instrument name.
    datetime_obs : str
        The observation date in ISO format.
    koa_id : str
        The KOA ID.

    Returns
    -------
    str
        The KOA filehandle.
    """
    year = datetime_obs[:4]
    ymd = datetime_obs[:10].replace('-', '')
    koa_filehandle = f"/{instrument_name}/{year}/{ymd}/{koa_id}"
    return koa_filehandle

def postgres_http_date_to_iso(date_str: str) -> str:
    """
    Return datetime as:
        YYYY-MM-DDTHH:MM:SS.SSS

    Parameters
    ----------
    date_str : str
        The input date string, which can be in one of the following formats:
    
            - ISO 8601 strings
            - Postgres HTTP-date strings like:
            
                'Thu, 12 Feb 2026 00:00:00 GMT'
    
    Returns
    -------
    str
        The datetime string in ISO format.
    """

    # Try ISO first
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        # Try Postgres HTTP-date
        try:
            dt = datetime.strptime(
                date_str,
                "%a, %d %b %Y %H:%M:%S GMT"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            raise ValueError(f"Invalid datetime string: {date_str}")

    # Convert to UTC if tz-aware
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)

    # Return exactly millisecond precision, no timezone
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}"