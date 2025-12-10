from geo_skeletons import PointSkeleton, GriddedSkeleton
import numpy as np


def test_dx_dy_cartesian():
    points = PointSkeleton(x=range(10), y=range(50,60))
    points.dy()
    np.testing.assert_array_almost_equal(points.dy(), np.sqrt(2))
    np.testing.assert_array_almost_equal(points.dx(), np.sqrt(2))


def test_dx_dy_spherical():
    points = PointSkeleton(lon=range(10), lat=range(50,60))
    points.dy()
    
    np.testing.assert_array_almost_equal(points.dy(), 128379, decimal=0)
    np.testing.assert_array_almost_equal(points.dx(), 128379, decimal=0)

def test_dx_dy_compare_point_gridded():
    points = PointSkeleton(lon=range(10), lat=0)
    grid = GriddedSkeleton(lon=range(10), lat=0)
    np.testing.assert_almost_equal(points.dx()/1000, grid.dx()/1000, decimal=1)
    
    np.testing.assert_almost_equal(points.dlon(), grid.dlon(), decimal=3)

    points = PointSkeleton(x=range(10), y=0)
    grid = GriddedSkeleton(x=range(10), y=0)
    np.testing.assert_almost_equal(points.dx(), grid.dx(), decimal=7)
    