import geo_parameters as gp
from geo_skeletons import PointSkeleton, GriddedSkeleton
from geo_skeletons.decorators import add_datavar, add_magnitude
import pytest 
import numpy as np

@pytest.fixture
def wind():
    @add_magnitude(gp.wind.Wind('ff'), x='u', y='v', direction=gp.wind.WindDir('dd'))
    @add_datavar(gp.wind.YWind("v"))
    @add_datavar(gp.wind.XWind("u"))
    class WindData(PointSkeleton):
        pass

    data = WindData(lon=range(10), lat=range(10))
    data.set_ff(10)
    data.set_dd(180)

    return data

@pytest.fixture
def windgrid():
    @add_magnitude(gp.wind.Wind('ff'), x='u', y='v', direction=gp.wind.WindDir('dd'))
    @add_datavar(gp.wind.YWind("v"))
    @add_datavar(gp.wind.XWind("u"))
    class WindData(GriddedSkeleton):
        pass

    data = WindData(lon=range(10), lat=range(10))
    data.set_ff(10)
    data.set_dd(180)

    return data


@pytest.fixture
def windto():
    @add_magnitude(gp.wind.Wind('ff'), x='u', y='v', direction=gp.wind.WindDirTo('dd'))
    @add_datavar(gp.wind.YWind("v"))
    @add_datavar(gp.wind.XWind("u"))
    class WindData(PointSkeleton):
        pass

    data = WindData(lon=range(10), lat=range(10))
    data.set_ff(10)
    data.set_dd(0)

    return data

@pytest.fixture
def windgridto():
    @add_magnitude(gp.wind.Wind('ff'), x='u', y='v', direction=gp.wind.WindDirTo('dd'))
    @add_datavar(gp.wind.YWind("v"))
    @add_datavar(gp.wind.XWind("u"))
    class WindData(GriddedSkeleton):
        pass

    data = WindData(lon=range(10), lat=range(10))
    data.set_ff(10)
    data.set_dd(0)

    return data


def test_get_dir_type_point(wind):
    np.testing.assert_array_almost_equal(wind.dd(),180)
    np.testing.assert_array_almost_equal(wind.dd(dir_type='from'),180)
    np.testing.assert_array_almost_equal(wind.dd(dir_type='to'),0)

def test_get_dir_type_point_gp(wind):
    np.testing.assert_array_almost_equal(wind.get(gp.wind.WindDir),180)
    np.testing.assert_array_almost_equal(wind.get(gp.wind.WindDir, dir_type='from'),180)
    np.testing.assert_array_almost_equal(wind.get(gp.wind.WindDir, dir_type='to'),0)

def test_get_dir_type_point_gp_opposite(wind):
    np.testing.assert_array_almost_equal(wind.get(gp.wind.WindDirTo),0)
    np.testing.assert_array_almost_equal(wind.get(gp.wind.WindDirTo, dir_type='from'),180)
    np.testing.assert_array_almost_equal(wind.get(gp.wind.WindDirTo, dir_type='to'),0)

def test_get_dir_type_grid(windgrid):
    np.testing.assert_array_almost_equal(windgrid.dd(),180)
    np.testing.assert_array_almost_equal(windgrid.dd(dir_type='from'),180)
    np.testing.assert_array_almost_equal(windgrid.dd(dir_type='to'),0)

def test_get_dir_type_grid_gp(windgrid):
    np.testing.assert_array_almost_equal(windgrid.get(gp.wind.WindDir),180)
    np.testing.assert_array_almost_equal(windgrid.get(gp.wind.WindDir, dir_type='from'),180)
    np.testing.assert_array_almost_equal(windgrid.get(gp.wind.WindDir, dir_type='to'),0)

def test_get_dir_type_grid_gp_opposite(windgrid):
    np.testing.assert_array_almost_equal(windgrid.get(gp.wind.WindDirTo),0)
    np.testing.assert_array_almost_equal(windgrid.get(gp.wind.WindDirTo, dir_type='from'),180)
    np.testing.assert_array_almost_equal(windgrid.get(gp.wind.WindDirTo, dir_type='to'),0)




def test_get_dir_type_point_to(windto):
    np.testing.assert_array_almost_equal(windto.dd(),0)
    np.testing.assert_array_almost_equal(windto.dd(dir_type='from'),180)
    np.testing.assert_array_almost_equal(windto.dd(dir_type='to'),0)

def test_get_dir_type_point_gp_to(windto):
    np.testing.assert_array_almost_equal(windto.get(gp.wind.WindDir),180)
    np.testing.assert_array_almost_equal(windto.get(gp.wind.WindDir, dir_type='from'),180)
    np.testing.assert_array_almost_equal(windto.get(gp.wind.WindDir, dir_type='to'),0)

def test_get_dir_type_point_gp_opposite_to(windto):
    np.testing.assert_array_almost_equal(windto.get(gp.wind.WindDirTo),0)
    np.testing.assert_array_almost_equal(windto.get(gp.wind.WindDirTo, dir_type='from'),180)
    np.testing.assert_array_almost_equal(windto.get(gp.wind.WindDirTo, dir_type='to'),0)

def test_get_dir_type_grid_to(windgridto):
    np.testing.assert_array_almost_equal(windgridto.dd(),0)
    np.testing.assert_array_almost_equal(windgridto.dd(dir_type='from'),180)
    np.testing.assert_array_almost_equal(windgridto.dd(dir_type='to'),0)

def test_get_dir_type_grid_gp_to(windgridto):
    np.testing.assert_array_almost_equal(windgridto.get(gp.wind.WindDir),180)
    np.testing.assert_array_almost_equal(windgridto.get(gp.wind.WindDir, dir_type='from'),180)
    np.testing.assert_array_almost_equal(windgridto.get(gp.wind.WindDir, dir_type='to'),0)

def test_get_dir_type_grid_gp_opposite_to(windgridto):
    np.testing.assert_array_almost_equal(windgridto.get(gp.wind.WindDirTo),0)
    np.testing.assert_array_almost_equal(windgridto.get(gp.wind.WindDirTo, dir_type='from'),180)
    np.testing.assert_array_almost_equal(windgridto.get(gp.wind.WindDirTo, dir_type='to'),0)
