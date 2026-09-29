from geo_skeletons.classes import WindGrid
import pytest
import numpy as np

def test_component_direction_default_wgs84():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(lat=10, lon=(-180, 180), crs=proj4)
    
    wind.set_spacing(nx=181)

    wind.set_ff(10)
    wind.set_dd(180)
    
    assert wind.proj.values_are_projected('u')
    assert wind.proj.values_are_projected('v')
    
    u = wind.u() # This gives in WGS84 by default
    v = wind.v()

    np.testing.assert_almost_equal(u, 0)
    np.testing.assert_almost_equal(v, 10)

    u_rot = wind.u(projected=True)
    v_rot = wind.v(projected=True)


    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(u, u_rot)
    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(v, v_rot)


    u_nat = wind.u(native=True)
    v_nat = wind.v(native=True)

    np.testing.assert_array_almost_equal(u_nat, u_rot)
    np.testing.assert_array_almost_equal(v_nat, v_rot)