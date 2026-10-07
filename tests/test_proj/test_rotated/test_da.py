from geo_skeletons.classes import WindGrid
import numpy as np

def test_reproj_southerly_winds_rotated_grid_lon0_0_u():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(lat=10, lon=(-180, 180))
    wind.set_spacing(nx=181)
    wind.set_ff(10)
    wind.set_dd(180)

    np.testing.assert_array_almost_equal(wind.dd(wrt=proj4, data_array=True).values, wind.dd(wrt=proj4))
    assert np.min(wind.dd(wrt=proj4, data_array=True).values) < 180
    np.testing.assert_array_almost_equal(wind.dd(data_array=True).values, wind.dd())
    
    np.testing.assert_array_almost_equal(wind.u(wrt=proj4, data_array=True).values, wind.u(wrt=proj4))
    assert np.min(wind.u(wrt=proj4, data_array=True).values) < 0
    np.testing.assert_array_almost_equal(wind.u(data_array=True).values, wind.u())

    np.testing.assert_array_almost_equal(wind.v(wrt=proj4, data_array=True).values, wind.v(wrt=proj4))
    assert np.min(wind.v(wrt=proj4, data_array=True).values) < 10
    np.testing.assert_array_almost_equal(wind.v(data_array=True).values, wind.v())


def test_reproj_from_rotated():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(x=(-8194139, 8194139), y=(-8194139, 8194139), crs=proj4)
    wind.set_spacing(nx=10, ny=15)
    
    wind.set_ff(10)

    wind.set_dd(180)
    np.testing.assert_array_almost_equal(wind.dd(wrt='crs', data_array=True).values, wind.dd(wrt='crs'))
    assert np.min(wind.dd(wrt='crs', data_array=True).values) < 180
    np.testing.assert_array_almost_equal(wind.dd(data_array=True).values, wind.dd())
    
    np.testing.assert_array_almost_equal(wind.u(wrt='crs', data_array=True).values, wind.u(wrt='crs'))
    assert np.min(wind.u(wrt='crs', data_array=True).values) < 0
    np.testing.assert_array_almost_equal(wind.u(data_array=True).values, wind.u())

    np.testing.assert_array_almost_equal(wind.v(wrt='crs', data_array=True).values, wind.v(wrt='crs'))
    assert np.min(wind.v(wrt='crs', data_array=True).values) < 10
    np.testing.assert_array_almost_equal(wind.v(data_array=True).values, wind.v())
