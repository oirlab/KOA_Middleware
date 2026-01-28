import os
import requests
from tqdm import tqdm
from .logging_utils import logger


login_url = "https://koa.ipac.caltech.edu/cgi-bin/KoaAPI/nph-koaLogin?"
getkoa_url = "https://koa.ipac.caltech.edu/cgi-bin/getKOA/nph-getKOA?return_mode=json&"
caliblist_url = "https://koa.ipac.caltech.edu/cgi-bin/KoaAPI/nph-getCaliblist?"

BASE_URL_KECK = "https://www3.keck.hawaii.edu/api/calibrations/"

def download_koa(
    koa_filename : str,
    output_dir : str,
    cookies : str | None = None
) -> str:
    """
    Download a file from the Keck Observatory Archive (KOA).

    Parameters
    ----------
    koa_filename : str
        The KOA filename to download.
    output_dir : str
        The directory to save the downloaded file.
    cookies : str, optional
        Optional cookies to include in the HTTP request.

    Returns
    -------
    str
        The local file path of the downloaded file.
    """

    # Make the directory
    os.makedirs(output_dir, exist_ok=True)

    # Local filename
    filename_local = os.path.join(output_dir, os.path.basename(koa_filename))

    # url
    url = getkoa_url + koa_filename

    # HTTP Request
    response = requests.get(url, stream=True, cookies=cookies)

    if response.status_code != 200:
        logger.error(f"Error downloading {koa_filename}: HTTP {response.status_code}")
    else:
        # Get total file size from headers if available
        total_size = int(response.headers.get('content-length', 0))

        # Save the file with a progress bar
        with open(filename_local, 'wb') as f, tqdm(
            total=total_size, unit='B', unit_scale=True, desc=filename_local
        ) as pbar:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

    # Return
    return filename_local
