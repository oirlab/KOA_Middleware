from koa_middleware.file_utils import generate_koa_id, generate_koa_filepath

def test_generate_koa_id():
    instrument_prefix = 'HB'
    date_obs = '20240924'
    utc_obs = '12:34:56.78'
    koa_id = generate_koa_id(instrument_prefix, date_obs, utc_obs)
    assert koa_id == 'HB.20240924.45296.78.fits'


def test_generate_koa_filepath():
    
    instrument = 'HISPEC'
    instrument_prefix = 'HB'
    data_level = 'L1'
    date_obs = '20240924'
    utc_obs = '12:34:56.78'
    koa_filepath = generate_koa_filepath(instrument, instrument_prefix, data_level, date_obs, utc_obs)
    assert koa_filepath == '/TEST_INSTRUMENT/2024/20240924/L1/HB.20240924.45296.78.fits'