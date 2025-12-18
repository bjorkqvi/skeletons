from geo_skeletons.classes import WindGrid

def test_compile_ds():
    data = WindGrid(lon=(10,20), lat=(50,60))
    data.set_u(2)
    data.set_v(3)

    ds = data.ds()
    assert set(ds.data_vars) ==  {'crs', 'u', 'v', 'wgs84'}

    ds = data.ds(compile=True)
    assert set(ds.data_vars) ==  {'crs', 'u', 'v', 'wgs84', 'ff','dd'}
    assert ds.crs.attrs == {'utm_zone': '32', 'utm_letter': 'U'}