from ..keck_client import KeckObserverAuthClient
from ..logging_utils import logger
import requests
from tqdm import tqdm
import zipfile
import os

__all__ = ['RemoteCalibrationDB']

_KECK_CALIBRATIONS_URL = "https://www3.keck.hawaii.edu/api/calibrations"

class RemoteCalibrationDB:
    """
    A class to interface with a remote calibration database hosted at Keck Observatory.
    
    The current implementation uses the Keck Observer login system for authentication.
    Eventually, this will be replaced with the appropriate client for accessing KOA.
    The URL `_KECK_CALIBRATIONS_URL` will also be replaced with the appropriate KOA URL.
    """

    def __init__(self, instrument_name: str):
        """
        Initialize a RemoteCalibrationDB instance.
        
        Parameters
        ----------
        instrument_name : str
            The name of the instrument (e.g., 'hispec', 'liger').
        """
        self.instrument_name = instrument_name.lower()
        self.auth_client = KeckObserverAuthClient()
        self.calibrations_url = os.environ.get('KOA_CALIBRATIONS_URL', _KECK_CALIBRATIONS_URL)
    
    #######################
    #### DOWNLOAD CALS ####
    #######################

    # def download_calibration(
    #     self,
    #     cal_id: str,
    #     output_dir: str,
    #     output_path: str | None = None,
    # ) -> str:
    #     os.makedirs(output_dir, exist_ok=True)

    #     route = f"{self.calibrations_url}/{self.instrument_name}/download"
    #     r = requests.get(
    #         route,
    #         params={"cal_id": cal_id},
    #         cookies=self.auth_client.cookies
    #     )

    #     if r.status_code != 200:
    #         msg = f"Failed to download calibration {cal_id}: {r.status_code} {r.text}"
    #         logger.error(msg)
    #         raise RuntimeError(msg)

    #     # Save zip to temporary location
    #     temp_zip = os.path.join(output_dir, f"{cal_id}.zip")
    #     with open(temp_zip, "wb") as f:
    #         f.write(r.content)

    #     # Extract and validate zip
    #     try:
    #         with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
    #             extracted_files = zip_ref.namelist()
                
    #             if not extracted_files:
    #                 msg = f"Zip archive for calibration {cal_id} is empty"
    #                 logger.error(msg)
    #                 raise RuntimeError(msg)
                
    #             filename_in_zip = next(
    #                 (f for f in extracted_files if not f.endswith('/')),
    #                 extracted_files[0]
    #             )
                
    #             zip_ref.extractall(output_dir)
                
    #         # Determine final output path
    #         if output_path is None:
    #             output_path = os.path.join(output_dir, filename_in_zip)
            
    #         if not os.path.exists(output_path):
    #             msg = f"Extracted calibration file not found at {output_path}"
    #             logger.error(msg)
    #             raise RuntimeError(msg)
            
    #         logger.info(f"Successfully downloaded calibration {cal_id} to {output_path}")
    #         return output_path
            
    #     except zipfile.BadZipFile as e:
    #         logger.error(f"Downloaded file {temp_zip} is not a valid zip archive: {e}")
    #         raise RuntimeError(f"Invalid zip archive for calibration {cal_id}") from e
    #     finally:
    #         # Always delete the temp zip file
    #         if os.path.exists(temp_zip):
    #             os.remove(temp_zip)

    def download_calibration(
        self,
        cal_id: str,
        output_dir: str,
        output_path: str | None = None,
    ) -> str:
        os.makedirs(output_dir, exist_ok=True)

        route = f"{self.calibrations_url}/{self.instrument_name}/download"
        r = requests.get(
            route,
            params={"cal_id": cal_id},
            cookies=self.auth_client.cookies,
            stream=True,
        )

        if r.status_code != 200:
            msg = f"Failed to download calibration {cal_id}: {r.status_code} {r.text}"
            logger.error(msg)
            raise RuntimeError(msg)

        temp_zip = os.path.join(output_dir, f"{cal_id}.zip")

        total_size = int(r.headers.get("content-length", 0))
        chunk_size = 8192

        with open(temp_zip, "wb") as f, tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=f"Downloading {cal_id}",
        ) as pbar:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

        try:
            with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
                extracted_files = zip_ref.namelist()
                
                if not extracted_files:
                    msg = f"Zip archive for calibration {cal_id} is empty"
                    logger.error(msg)
                    raise RuntimeError(msg)
                
                filename_in_zip = next(
                    (f for f in extracted_files if not f.endswith('/')),
                    extracted_files[0]
                )
                
                zip_ref.extractall(output_dir)
                
            if output_path is None:
                output_path = os.path.join(output_dir, filename_in_zip)
            
            if not os.path.exists(output_path):
                msg = f"Extracted calibration file not found at {output_path}"
                logger.error(msg)
                raise RuntimeError(msg)
            
            logger.info(f"Successfully downloaded calibration {cal_id} to {output_path}")
            return output_path
            
        except zipfile.BadZipFile as e:
            logger.error(f"Downloaded file {temp_zip} is not a valid zip archive: {e}")
            raise RuntimeError(f"Invalid zip archive for calibration {cal_id}") from e
        finally:
            if os.path.exists(temp_zip):
                os.remove(temp_zip)

    ########################
    #### QUERY METADATA ####
    ########################

    def query(self, **kwargs) -> dict | list[dict]:
        """
        Query metadata from the remote calibration database.

        Parameters
        ----------
        **kwargs
            cal_type : str, optional
                Calibration type to filter by (e.g., "dark").
            cal_id : str, optional
                Calibration ID to filter by.
            date_time_start : str, optional
                Start datetime in ISO format.
            date_time_end : str, optional
                End datetime in ISO format.
            last_updated_start : str, optional
                Start of last_updated range in ISO format.
            last_updated_end : str, optional
                End of last_updated range in ISO format.
            fetch : str, optional
                Fetch mode; use "first" to return only the first result.
            Additional kwargs
                Other parameters to pass to the remote API.

        Returns
        -------
        dict or list[dict]
            The JSON response containing the queried metadata.
        """
        route = f"{self.calibrations_url}/{self.instrument_name.lower()}/query"
        response = requests.get(route, params=kwargs, cookies=self.auth_client.cookies)
        if response.status_code != 200:
            msg = f"Failed to query metadata: {response.status_code} {response.text}"
            logger.error(msg)
            raise RuntimeError(msg)
        out = response.json()
        if isinstance(out, dict) and out.get('message') == 'No matching calibrations found.':
            return []
        return out

    def get_last_updated(self) -> str:
        """
        Get the last updated timestamp for the instrument's calibration data.

        Returns
        -------
        str
            The last updated timestamp as an ISO format string.
        """
        route = f"{self.calibrations_url}/{self.instrument_name.lower()}/lastUpdated"
        response = requests.get(route, cookies=self.auth_client.cookies)
        if response.status_code != 200:
            msg = f"Failed to get last updated info: {response.status_code} {response.text}"
            logger.error(msg)
            raise RuntimeError(msg)
        data = response.json()
        return data["last_updated"]
    
    ###################################
    #### ADD NEW CALIBRATION TO DB ####
    ###################################

    def add(self, meta : dict | list[dict]):
        """
        Add a new calibration metadata entry or entries to the remote database.

        Parameters
        ----------
        meta : dict or list[dict]
            A dictionary or a list of dictionaries containing the calibration metadata to add.
            If a list is provided, each dictionary should represent a separate calibration entry.
        """
        
        if isinstance(meta, dict):
            meta = [meta]

        
        # HACK: This is a temporary hack to convert boolean cols from 1/0 (sqlite) to True/False.
        # NOTE: Consider implementing a conversion on the backend.
        boolean_cols = ['master_cal']
        for m in meta:
            for col in boolean_cols:
                if col in m:
                    m[col] = bool(m[col])

        route = f"{self.calibrations_url}/{self.instrument_name.lower()}/add"
        response = requests.post(route, json=meta, cookies=self.auth_client.cookies)
        if response.status_code != 200:
            msg = f"Failed to add calibration metadata: {response.status_code} {response.text}"
            logger.error(msg)
            raise RuntimeError(msg)
        
        logger.info(f"Successfully added {len(meta)} calibration entries to remote database.")
        return response.json()
    
    def __repr__(self):
        return f"RemoteCalibrationDB(instrument_name={self.instrument_name!r})"
    
    @staticmethod
    def _credentials_available() -> bool:
        email = os.getenv("KECK_OBSERVER_EMAIL")
        password = os.getenv("KECK_OBSERVER_PASSWORD")
        return bool(email and password)