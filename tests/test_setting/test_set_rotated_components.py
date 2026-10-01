from geo_skeletons.classes import WindGrid
from geo_skeletons import GriddedSkeleton
import pytest
from geo_skeletons.errors import ProjectionError
import geo_parameters as gp
import numpy as np
from geo_skeletons.decorators import add_datavar, add_magnitude



def test_mag_ok():
    wind = WindGrid(lon=0, lat=0)
    wind.set_ff(10, projected=True)
    assert wind.ff() == 10
    wind.set_ff(10, projected=False)
    assert wind.ff() == 10
    wind.set_ff(10)
    assert wind.ff() == 10



def test_dummy_ok():
    wind = GriddedSkeleton.add_datavar('ff')(lon=0, lat=0)
    wind.set_ff(10, projected=True)
    assert wind.ff() == 10
    wind.set_ff(10, projected=False)
    assert wind.ff() == 10
    wind.set_ff(10)
    assert wind.ff() == 10


def test_no_gp_components():
    @add_datavar('v')
    @add_datavar('u')
    class NoGP(GriddedSkeleton):
        pass

    # adding a magnitude should automatically give u,v the wgs84 projection

    wind = NoGP(lon=0, lat=0)
    assert not wind.core.values_are_projected('u')

    @add_magnitude('ff', x='u', y='v', direction='dd', dir_type='from')
    @add_datavar('v')
    @add_datavar('u')
    class NoGP(GriddedSkeleton):
        pass

    # adding a magnitude should automatically give u,v the wgs84 projection
    wind = NoGP(lon=0, lat=0)
    assert not wind.core.values_are_projected('u')
    assert not wind.core.values_are_projected('v')

def test_mixing_components_raises_error():
    wind = WindGrid(lon=0, lat=0)
    wind.set_u(10, projected=False)
    with pytest.raises(ProjectionError):
        wind.set_v(10, projected=True)

def test_components_mag():
    wind = WindGrid(lon=0, lat=0)

    wind.set_u(1, projected=True)
    assert wind._u_v_buffer.get('u') is None
    wind.set_v(3, projected=True)
    assert wind._u_v_buffer.get('u') is None
    assert wind._u_v_buffer.get('v') is None
    np.testing.assert_almost_equal(wind.ff(), 10**0.5)
    
    wind.set_u(1, projected=False)
    assert wind._u_v_buffer.get('u') is not None
    wind.set_v(3, projected=False)
    assert wind._u_v_buffer.get('u') is None
    assert wind._u_v_buffer.get('v') is None
    np.testing.assert_almost_equal(wind.ff(), 10**0.5)
    

def test_set_projected_components():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    
    wind = WindGrid(lat=10, lon=(-180, 180), crs=proj4)
    
    wind.set_spacing(nx=181)
    
    wind.set_ff(10)
    wind.set_dd(180)
    


    dd = wind.dd()
    dd_rot = wind.dd(projected=True)
    ff = wind.ff()

    urot = wind.u()
    vrot = wind.v()

    u = wind.u(projected=False)
    v = wind.v(projected=False)
    
    wind.set_u(u, projected=False)
    wind.set_v(v, projected=False)
    
    assert wind._u_v_buffer.get('u') is None
    wind.set_v(vrot, projected=True)
    
    np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    np.testing.assert_array_almost_equal(wind.u(), urot)
    np.testing.assert_array_almost_equal(wind.v(), vrot)
    
    wind.set_u(u, projected=False) # Stays in buffer
    np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    np.testing.assert_array_almost_equal(wind.u(projected=False), u)
    np.testing.assert_array_almost_equal(wind.v(projected=False), v)
    wind.set_u(urot, projected=True)
    wind.set_v(vrot, projected=True)

    np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    np.testing.assert_array_almost_equal(wind.u(projected=False), u)
    np.testing.assert_array_almost_equal(wind.v(projected=False), v)

    # Setting wrong data
    wind.set_u(u)
    wind.set_v(v)

    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(wind.u(), urot)
    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(wind.v(), vrot)

