from geo_skeletons.classes import WindGrid
from geo_skeletons import GriddedSkeleton
import pytest
from geo_skeletons.errors import ProjectionError
import geo_parameters as gp
import numpy as np
from geo_skeletons.decorators import add_datavar, add_magnitude


def test_native_projected_same_time_raises_error():
    wind = WindGrid(lon=0, lat=0)
    with pytest.raises(ValueError):
        wind.set_u(0, native=True, projected=True)

def test_mag_ok():
    wind = WindGrid(lon=0, lat=0)
    wind.set_ff(10, projected=True)
    assert wind.ff() == 10
    wind.set_ff(10, projected=False)
    assert wind.ff() == 10
    wind.set_ff(10, native=False)
    assert wind.ff() == 10
    wind.set_ff(10, native=True)
    assert wind.ff() == 10


def test_dummy_ok():
    wind = GriddedSkeleton.add_datavar('ff')(lon=0, lat=0)
    wind.set_ff(10, projected=True)
    assert wind.ff() == 10
    wind.set_ff(10, projected=False)
    assert wind.ff() == 10
    wind.set_ff(10, native=False)
    assert wind.ff() == 10
    wind.set_ff(10, native=True)
    assert wind.ff() == 10


def test_no_gp_components():
    @add_datavar('v')
    @add_datavar('u')
    class NoGP(GriddedSkeleton):
        pass

    # adding a magnitude should automatically give u,v the wgs84 projection

    wind = NoGP(lon=0, lat=0)
    with pytest.raises(ProjectionError):
        assert wind.proj.values_are_projected('u')

    @add_magnitude('ff', x='u', y='v', direction='dd', dir_type='from')
    @add_datavar('v')
    @add_datavar('u')
    class NoGP(GriddedSkeleton):
        pass

    # adding a magnitude should automatically give u,v the wgs84 projection
    wind = NoGP(lon=0, lat=0)
    assert not wind.proj.values_are_projected('u')
    assert not wind.proj.values_are_projected('v')

def test_mixing_components_raises_error():
    wind = WindGrid(lon=0, lat=0)
    wind.set_u(10, projected=True)
    with pytest.raises(ProjectionError):
        wind.set_v(10, projected=False)

def test_components_mag():
    wind = WindGrid(lon=0, lat=0)

    wind.set_u(1, projected=True)
    assert wind._u_v_buffer.get('u') is not None
    wind.set_v(3, projected=True)
    assert wind._u_v_buffer.get('u') is None
    assert wind._u_v_buffer.get('v') is None
    np.testing.assert_almost_equal(wind.ff(), 10**0.5)
    


def test_set_projected_components():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    
    wind = WindGrid(lat=10, lon=(-180, 180), crs=proj4)
    
    wind.set_spacing(nx=181)
    
    wind.set_ff(10)
    breakpoint()
    wind.set_dd(180)
    


    dd = wind.dd()
    dd_rot = wind.dd(projected=True)
    ff = wind.ff()

    urot = wind.u(projected=True)
    vrot = wind.v(projected=True)

    u = wind.u()
    v = wind.v()
    
    wind.set_u(u)
    wind.set_v(v)
    
    assert wind._u_v_buffer.get('u') is None
    wind.set_v(v)
    
    np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    np.testing.assert_array_almost_equal(wind.u(), u)
    np.testing.assert_array_almost_equal(wind.v(), v)

    wind.set_u(u, projected=True) # Stays in buffer
    np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    np.testing.assert_array_almost_equal(wind.u(), u)
    np.testing.assert_array_almost_equal(wind.v(), v)
    wind.set_u(urot, projected=True)
    wind.set_v(vrot, projected=True)

    np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    np.testing.assert_array_almost_equal(wind.u(), u)
    np.testing.assert_array_almost_equal(wind.v(), v)

    wind.set_u(u, projected=True)
    wind.set_v(v, projected=True)

    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(wind.u(), u)
    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(wind.v(), v)

