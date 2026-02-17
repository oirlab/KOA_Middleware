from koa_middleware.utils import generate_koa_filehandle, get_koa_id_timestamp_from_datetime

def test_generate_koa_filehandle():
    
    instrument_name = 'HISPEC'
    instrument_prefix = 'HB'
    datetime_obs = '2024-09-24T12:34:56.780'
    dt = get_koa_id_timestamp_from_datetime(datetime_obs)
    koa_id = f"{instrument_prefix}.{dt}.fits"
    koa_filehandle = generate_koa_filehandle(instrument_name, datetime_obs, koa_id)
    assert koa_filehandle == '/HISPEC/2024/20240924/HB.20240924.45296.78.fits'