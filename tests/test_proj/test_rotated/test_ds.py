from geo_skeletons.classes import WindGrid
import numpy as np

def test_reproj_southerly_winds_rotated_grid_lon0_0_u():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(lat=10, lon=(-180, 180))
    wind.set_spacing(nx=181)
    wind.set_ff(10)
    wind.set_dd(180)
    ds_rot = wind.ds(compile=True, wrt=proj4)
    
    ds_lonlat = wind.ds(compile=True, wrt='lonlat')
    ds = wind.ds(compile=True)
    np.testing.assert_array_almost_equal(ds_rot.dd.values, wind.dd(wrt=proj4, squeeze=False))
    np.testing.assert_array_almost_equal(ds_rot.u.values, wind.u(wrt=proj4, squeeze=False))
    np.testing.assert_array_almost_equal(ds_rot.v.values, wind.v(wrt=proj4, squeeze=False))
    np.testing.assert_array_almost_equal(ds_lonlat.dd.values, wind.dd(wrt='lonlat', squeeze=False))
    np.testing.assert_array_almost_equal(ds_lonlat.u.values, wind.u(wrt='lonlat', squeeze=False))
    np.testing.assert_array_almost_equal(ds_lonlat.v.values, wind.v(wrt='lonlat', squeeze=False))
    np.testing.assert_array_almost_equal(ds.dd.values, wind.dd(wrt='lonlat', squeeze=False))
    np.testing.assert_array_almost_equal(ds.u.values, wind.u(squeeze=False))
    np.testing.assert_array_almost_equal(ds.v.values, wind.v(squeeze=False))

def test_reproj_from_rotated():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(x=(-8194139, 8194139), y=(-8194139, 8194139), crs=proj4)
    wind.set_spacing(nx=10, ny=15)

    wind.set_ff(10)
    wind.set_dd(180)
    ds_rot = wind.ds(compile=True, wrt='xy')
    
    ds_lonlat = wind.ds(compile=True, wrt='lonlat')
    ds = wind.ds(compile=True)
    np.testing.assert_array_almost_equal(ds_rot.dd.values, wind.dd(wrt=proj4, squeeze=False))
    np.testing.assert_array_almost_equal(ds_rot.u.values, wind.u(wrt=proj4, squeeze=False))
    np.testing.assert_array_almost_equal(ds_rot.v.values, wind.v(wrt=proj4, squeeze=False))
    np.testing.assert_array_almost_equal(ds_lonlat.dd.values, wind.dd(wrt='lonlat', squeeze=False))
    np.testing.assert_array_almost_equal(ds_lonlat.u.values, wind.u(wrt='lonlat', squeeze=False))
    np.testing.assert_array_almost_equal(ds_lonlat.v.values, wind.v(wrt='lonlat', squeeze=False))
    np.testing.assert_array_almost_equal(ds.dd.values, wind.dd(wrt='lonlat', squeeze=False))
    np.testing.assert_array_almost_equal(ds.u.values, wind.u(wrt='xy',squeeze=False))
    np.testing.assert_array_almost_equal(ds.v.values, wind.v(wrt='xy', squeeze=False))
