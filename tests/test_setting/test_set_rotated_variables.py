from geo_skeletons.classes import WindGrid
from geo_skeletons import GriddedSkeleton
import pytest
from geo_skeletons.errors import ProjectionError
import geo_parameters as gp
import numpy as np
from geo_skeletons.decorators import add_datavar, add_magnitude



def test_mag_ok():
    wind = WindGrid(lon=0, lat=0)
    wind.set_ff(10, wrt='lonlat')
    assert wind.ff() == 10
    wind.set_ff(10, wrt='xy')
    assert wind.ff() == 10
    wind.set_ff(10, wrt=(33,'W'))
    assert wind.ff() == 10



def test_dummy_ok():
    wind = GriddedSkeleton.add_datavar('ff')(lon=0, lat=0)
    wind.set_ff(10, wrt='lonlat')
    assert wind.ff() == 10
    wind.set_ff(10, wrt='xy')
    assert wind.ff() == 10
    wind.set_ff(10, wrt=(33,'W'))
    assert wind.ff() == 10




# def test_no_gp_components():
#     @add_datavar('v')
#     @add_datavar('u')
#     class NoGP(GriddedSkeleton):
#         pass

#     # adding a magnitude should automatically give u,v the wgs84 projection

#     wind = NoGP(lon=0, lat=0)
#     assert not wind.core.values_are_projected('u')

#     @add_magnitude('ff', x='u', y='v', direction='dd', dir_type='from')
#     @add_datavar('v')
#     @add_datavar('u')
#     class NoGP(GriddedSkeleton):
#         pass

#     # adding a magnitude should automatically give u,v the wgs84 projection
#     wind = NoGP(lon=0, lat=0)
#     assert not wind.core.values_are_projected('u')
#     assert not wind.core.values_are_projected('v')

def test_mixing_components_raises_error():
    wind = WindGrid(x=0, y=0)
    wind.set_u(10, wrt='lonlat')

    with pytest.raises(ProjectionError):
        wind.set_v(10, wrt='xy')

def test_components_mag():
    wind = WindGrid(lon=0, lat=0)

    wind.set_u(1)
    assert wind._u_v_buffer.get('u') is None
    wind.set_v(3)
    assert wind._u_v_buffer.get('u') is None
    assert wind._u_v_buffer.get('v') is None
    np.testing.assert_almost_equal(wind.ff(), 10**0.5)
    
    wind.set_u(1, wrt=(33,'W'))
    assert wind._u_v_buffer.get('u') is not None
    wind.set_v(3, wrt=(33,'W'))
    assert wind._u_v_buffer.get('u') is None
    assert wind._u_v_buffer.get('v') is None
    np.testing.assert_almost_equal(wind.ff(), 10**0.5)


def test_set_directional_parameter():
    data = GriddedSkeleton.add_datavar(gp.wind.WindDir())(lon=15, lat=50)
    # data.set_dd(180, wrt='lonlat')
    # assert data.dd() == 180
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    x, y = data.xy(crs=proj4)
    xydata = GriddedSkeleton.add_datavar(gp.wind.WindDir())(x=x, y=y, crs=proj4)
    np.testing.assert_almost_equal(xydata.lon(), 15)
    np.testing.assert_almost_equal(xydata.lat(), 50)

    xydata.set_dd(180)
    np.testing.assert_almost_equal(xydata.dd(), 180)
    np.testing.assert_almost_equal(xydata.dd(wrt='xy'), 165)
    np.testing.assert_almost_equal(xydata.dd(wrt=proj4), 165)
    with pytest.raises(AssertionError):
        np.testing.assert_almost_equal(xydata.dd(wrt=(33,'W')), 165)
    xydata.set_dd(165, wrt='xy')
    np.testing.assert_almost_equal(xydata.dd(), 180)
    np.testing.assert_almost_equal(xydata.dd(wrt='xy'), 165)
    np.testing.assert_almost_equal(xydata.dd(wrt=proj4), 165)
    with pytest.raises(AssertionError):
        np.testing.assert_almost_equal(xydata.dd(wrt=(33,'W')), 165)

def test_set_projected_components():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    
    wind = WindGrid(lat=10, lon=(-180, 180))
    
    wind.set_spacing(nx=181)
    
    wind.set_ff(10)
    wind.set_dd(180)

    dd = wind.dd()
    ff = wind.ff()

    urot = wind.u(wrt=proj4)
    vrot = wind.v(wrt=proj4)

    u = wind.u()
    v = wind.v()
    
    wind.set_u(u)
    wind.set_v(v)
    
    assert wind._u_v_buffer.get('u') is None
    
    np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    np.testing.assert_array_almost_equal(wind.u(wrt=proj4), urot)
    np.testing.assert_array_almost_equal(wind.v(wrt=proj4), vrot)
    
    wind.set_u(urot, wrt=proj4) # Stays in buffer
    np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    np.testing.assert_array_almost_equal(wind.u(), u)
    np.testing.assert_array_almost_equal(wind.v(), v)
    wind.set_v(vrot, wrt=proj4)

    np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    np.testing.assert_array_almost_equal(wind.u(), u)
    np.testing.assert_array_almost_equal(wind.v(), v)
    np.testing.assert_array_almost_equal(wind.u(wrt=proj4), urot)
    np.testing.assert_array_almost_equal(wind.v(wrt=proj4), vrot)
    # Setting wrong data
    wind.set_u(urot)
    wind.set_v(vrot)

    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(wind.dd(), dd)
    np.testing.assert_array_almost_equal(wind.ff(), ff)
    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(wind.u(), u)
    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(wind.v(), v)

