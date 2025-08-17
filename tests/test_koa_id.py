from koa_middleware.file_utils import generate_koa_id

def test_generate_koa_id():
    instrument_prefix = 'HB'
    date_obs = '20240924'
    utc_obs = '12:34:56.78'
    koa_id = generate_koa_id(instrument_prefix, date_obs, utc_obs)
    assert koa_id == 'HB.20240924.45296.78.fits'