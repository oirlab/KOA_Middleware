import os
import requests
from tqdm import tqdm


login_url = "https://koa.ipac.caltech.edu/cgi-bin/KoaAPI/nph-koaLogin?"
getkoa_url = "https://koa.ipac.caltech.edu/cgi-bin/getKOA/nph-getKOA?return_mode=json&"
caliblist_url = "https://koa.ipac.caltech.edu/cgi-bin/KoaAPI/nph-getCaliblist?"


# NOTE: Possibly for later use, delete if not useful
#def koa_login(username : str | None, password : str, cookiepath : str):
#    return Koa.login(cookiepath=cookiepath, username=username, password=password)
    # # Encode login credentials
    # param = dict(userid=username, password=password)
    # data_encoded = urllib.parse.urlencode(param)
    
    # # URL
    # url = self.login_url + data_encoded
    
    # cookie_filename = 'cookie.txt'
    # try:
    #     Koa.login(username, password)
    # except Exception as e:
    #     print(e)
    #     return False

    # return cookie_filename


def download_koa(
        koa_filename : str,
        output_dir : str,
        cookies : str | None = None
    ) -> str:

    # Make the directory
    os.makedirs(output_dir, exist_ok=True)

    # Local filename
    filename_local = os.path.join(output_dir, os.path.basename(koa_filename))

    # url
    url = getkoa_url + koa_filename

    # HTTP Request
    response = requests.get(url, stream=True, cookies=cookies)

    if response.status_code != 200:
        print(f"Error: {response.status_code}")
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