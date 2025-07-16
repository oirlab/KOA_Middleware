from hispecdrp import datamodels

def generate_koa_filepath(input : datamodels.HISPECDataModel | str) -> str:
    """
    Returns the filepath on KOA for a given input data model. Use flat paths for now to match HISPEC_TEST_DATA.
    # KPF example = "/KPF/2024/20240926/lev0/KP.20240926.74183.78.fits"
    """
    model = datamodels.open(input, meta_only=True)
    if isinstance(model, datamodels.HISPECCalibrationModel):
        level = "cal"
    else:
        level = f"lev{model.meta.data_level}"
    date = model.meta.date_obs
    year = date[0:4]
    koa_id = generate_koa_id(model)
    return f"/HISPEC/{year}/{date}/{level}/{koa_id}"


def generate_koa_id(input : datamodels.HISPECDataModel | str) -> str:
    model = datamodels.open(input, meta_only=True)
    date = model.meta.date_obs
    utc = datetime.datetime.strptime(model.meta.utc, '%H:%M:%S.%f')
    total_seconds = utc.hour * 3600 + utc.minute * 60 + utc.second + utc.microsecond / 1e6
    time = f"{total_seconds:08.2f}"
    ext = os.path.splitext(model.meta.filename)[1]
    return f"HS.{date}.{time}{ext}"