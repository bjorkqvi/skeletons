from geo_skeletons.classes import WindGrid
import pytest
from geo_skeletons.errors import ProjectionError
import geo_parameters as gp
import numpy as np

def test_components_mag():
    wind = WindGrid(lon=0, lat=0)

    wind.set_u(1, rotated=True)
    assert wind._u_v_buffer.get('u') is not None
    wind.set_v(3, rotated=True)
    assert wind._u_v_buffer.get('u') is None
    assert wind._u_v_buffer.get('v') is None
    np.testing.assert_almost_equal(wind.ff(), 10**0.5)
    


def test_component_direction():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(lat=10, lon=(-180, 180), crs=proj4)
    
    wind.set_spacing(nx=181)

    wind.set_ff(10)
    wind.set_dd(180)

    u = wind.u()
    v = wind.v()
    dd = wind.dd()
    dd_rot = wind.dd(rotated=True)
    ff = wind.ff()

    urot = wind.u(rotated=True)
    vrot = wind.v(rotated=True)

    wind.set_u(u)
    wind.set_v(v)

    np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    np.testing.assert_array_almost_equal(wind.u(), u)
    np.testing.assert_array_almost_equal(wind.v(), v)

    wind.set_u(u, rotated=True) # Stays in buffer
    np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    np.testing.assert_array_almost_equal(wind.u(), u)
    np.testing.assert_array_almost_equal(wind.v(), v)
    wind.set_u(urot, rotated=True)
    wind.set_v(vrot, rotated=True)

    np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    np.testing.assert_array_almost_equal(wind.u(), u)
    np.testing.assert_array_almost_equal(wind.v(), v)

    wind.set_u(u, rotated=True)
    wind.set_v(v, rotated=True)

    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(wind.u(), u)
    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(wind.v(), v)

