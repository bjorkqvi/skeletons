from geo_skeletons.classes import WindGrid
import numpy as np

def test_reproj_southerly_winds_rotated_grid_lon0_0_u():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(lat=10, lon=(-180, 180), crs=proj4)
    wind.set_spacing(nx=181)
    wind.set_ff(10)
    wind.set_dd(180)

    ds_rot = wind.ds(compile=True, rotated=True)
    
    ds = wind.ds(compile=True)
    np.testing.assert_array_almost_equal(ds_rot.dd.values, wind.dd(rotated=True, squeeze=False))
    np.testing.assert_array_almost_equal(ds_rot.u.values, wind.u(rotated=True, squeeze=False))
    np.testing.assert_array_almost_equal(ds_rot.v.values, wind.v(rotated=True, squeeze=False))
    np.testing.assert_array_almost_equal(ds.dd.values, wind.dd(rotated=False, squeeze=False))
    np.testing.assert_array_almost_equal(ds.u.values, wind.u(rotated=False, squeeze=False))
    np.testing.assert_array_almost_equal(ds.v.values, wind.v(rotated=False, squeeze=False))

    assert ds_rot.dd.rotated_according_to =='crs'
    assert ds_rot.u.rotated_according_to =='crs'
    assert ds_rot.v.rotated_according_to =='crs'

    assert ds.dd.rotated_according_to =='wgs84'
    assert ds.u.rotated_according_to =='wgs84'
    assert ds.v.rotated_according_to =='wgs84'

    assert ds.dd.grid_mapping =='wgs84'
    assert ds.u.grid_mapping =='wgs84'
    assert ds.v.grid_mapping =='wgs84'

    assert ds_rot.dd.grid_mapping =='wgs84'
    assert ds_rot.u.grid_mapping =='wgs84'
    assert ds_rot.v.grid_mapping =='wgs84'


def test_reproj_from_rotated():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(x=(-8194139, 8194139), y=(-8194139, 8194139), crs=proj4)
    wind.set_spacing(nx=10, ny=15)

    wind.set_ff(10)
    wind.set_dd(180)
    ds_rot = wind.ds(compile=True, rotated=True)
    
    ds = wind.ds(compile=True)
    np.testing.assert_array_almost_equal(ds_rot.dd.values, wind.dd(rotated=True, squeeze=False))
    np.testing.assert_array_almost_equal(ds_rot.u.values, wind.u(rotated=True, squeeze=False))
    np.testing.assert_array_almost_equal(ds_rot.v.values, wind.v(rotated=True, squeeze=False))
    np.testing.assert_array_almost_equal(ds.dd.values, wind.dd(rotated=False, squeeze=False))
    np.testing.assert_array_almost_equal(ds.u.values, wind.u(rotated=False, squeeze=False))
    np.testing.assert_array_almost_equal(ds.v.values, wind.v(rotated=False, squeeze=False))

    assert ds_rot.dd.rotated_according_to =='crs'
    assert ds_rot.u.rotated_according_to =='crs'
    assert ds_rot.v.rotated_according_to =='crs'

    assert ds.dd.rotated_according_to =='wgs84'
    assert ds.u.rotated_according_to =='wgs84'
    assert ds.v.rotated_according_to =='wgs84'

    assert ds.dd.grid_mapping =='crs'
    assert ds.u.grid_mapping =='crs'
    assert ds.v.grid_mapping =='crs'

    assert ds_rot.dd.grid_mapping =='crs'
    assert ds_rot.u.grid_mapping =='crs'
    assert ds_rot.v.grid_mapping =='crs'