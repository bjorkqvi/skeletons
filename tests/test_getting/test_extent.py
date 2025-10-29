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
    np.testing.assert_almost_equal(distance_2points(0, 0, 0, 6), points.extent("x"))
    np.testing.assert_almost_equal(distance_2points(-10, 3, 10, 3), points.extent("y"))


def test_gridded_spherical():
    points = GriddedSkeleton(lon=(0, 6), lat=(-10, 10))
    np.testing.assert_almost_equal(distance_2points(0, 0, 0, 6), points.extent("x"))
    np.testing.assert_almost_equal(distance_2points(-10, 3, 10, 3), points.extent("y"))
