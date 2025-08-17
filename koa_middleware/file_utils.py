
import datetime
    
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
    - `year` is the year of the observation in YYYY.
    - `date_obs` is the observation date in 'YYYYMMDD' format.
    - `data_level` is the data level (e.g., 'lev0').
    - `koa_id` is the KOA ID in the format, see *generate_koa_id*.
    Example = "/HISPEC/2024/20240924/lev0/HR.20240924.12345.67.fits"

    Args:
        instrument (str): The instrument name.
        instrument_prefix (str): The two letter prefix for the instrument.
        data_level (str): The data level (e.g., 'lev0').
        date_obs (str): The observation date in 'YYYYMMDD' format.
        utc_obs (str): The UTC observation time in 'HH:MM:SS.sss' format.

    Returns:
        str: The KOA filepath for the given input data model.
    """
    year = date_obs[0:4]
    koa_id = generate_koa_id(instrument_prefix, date_obs, utc_obs)
    return f"/{instrument.upper()}/{year}/{date_obs}/{data_level}/{koa_id}"

def generate_koa_id(instrument_prefix : str, date_obs : str = None, utc_obs : str = None, ext : str = 'fits') -> str:
    """
    Generates the KOA ID.
    Format: "{instrument_prefix}.{date_obs}.{seconds}{ext}" where:
    - `instrument_prefix` is the two letter prefix for the instrument.
    - `date_obs` is the observation date in 'YYYYMMDD' format.
    - `seconds` is the total number of seconds since midnight UTC, formatted as 'SSSSS.ss'.
    - `ext` is the file extension, default is 'fits'.

    Example: HR.20240924.12345.67.fits

    Args:
        instrument_prefix (str): The two letter prefix for the instrument.
        date_obs (str): The observation date in 'YYYYMMDD' format.
        utc_obs (str): The UTC observation time in 'HH:MM:SS.sss' format.
        ext (str): The file extension, default is 'fits'.

    Returns:
        str: The KOA ID string.
    """
    utc = datetime.datetime.strptime(utc_obs, '%H:%M:%S.%f')
    total_seconds = utc.hour * 3600 + utc.minute * 60 + utc.second + utc.microsecond / 1e6
    seconds = f"{total_seconds:08.2f}"
    return f"{instrument_prefix}.{date_obs}.{seconds}{ext}"