from geo_skeletons import GriddedSkeleton, PointSkeleton
import numpy as np
from geo_skeletons.distance_funcs import distance_2points


def test_point_cartesian():
    points = PointSkeleton(x=(0, 100), y=(500, 800))
    np.testing.assert_almost_equal(np.diff(points.edges("x"))[0], points.extent("x"))
    np.testing.assert_almost_equal(np.diff(points.edges("y"))[0], points.extent("y"))


def test_gridded_cartesian():
    points = GriddedSkeleton(x=(0, 100), y=(500, 800))
    np.testing.assert_almost_equal(np.diff(points.edges("x"))[0], points.extent("x"))
    np.testing.assert_almost_equal(np.diff(points.edges("y"))[0], points.extent("y"))


def test_point_spherical():
    points = PointSkeleton(lon=(0, 6), lat=(-10, 10))
    d1 = distance_2points(-10, 0, -10,6)
    d2 = distance_2points(10, 0, 10,6)
    np.testing.assert_almost_equal((d1+d2)/2, points.extent("x"))
    d1 = distance_2points(-10, 0, 10,0)
    d2 = distance_2points(-10, 6, 10,6)
    np.testing.assert_almost_equal((d1+d2)/2, points.extent("y"))


def test_gridded_spherical():
    points = GriddedSkeleton(lon=(0, 6), lat=(-10, 10))
    d1 = distance_2points(-10, 0, -10,6)
    d2 = distance_2points(10, 0, 10,6)
    np.testing.assert_almost_equal((d1+d2)/2, points.extent("x"))
    d1 = distance_2points(-10, 0, 10,0)
    d2 = distance_2points(-10, 6, 10,6)
    np.testing.assert_almost_equal((d1+d2)/2, points.extent("y"))
