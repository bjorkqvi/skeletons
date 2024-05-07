from geo_skeletons.point_skeleton import PointSkeleton
from geo_skeletons.distance_funcs import distance_2points

import numpy as np


def test_yank_one_point_with_exact_coordinate():
    grid = PointSkeleton(lon=(10, 11), lat=(0, 1))
    yanked_points = grid.yank_point(lon=10, lat=0)
    assert len(yanked_points["inds"]) == 1
    assert len(yanked_points["dx"]) == 1
    assert yanked_points["inds"][0] == 0
    np.testing.assert_almost_equal(yanked_points["dx"][0], 0)


def test_yank_several_points_with_exact_coordinates():
    grid = PointSkeleton(lon=(10, 11, 12, 13, 14), lat=(0, 1, 2, 3, 4))
    yanked_points = grid.yank_point(lon=(10, 12, 14), lat=(0, 2, 4), fast=True)
    assert len(yanked_points["inds"]) == 3
    assert len(yanked_points["dx"]) == 3
    np.testing.assert_array_equal(yanked_points["inds"], np.array([0, 2, 4]))
    np.testing.assert_array_almost_equal(yanked_points["dx"], np.array([0, 0, 0]))


def test_yank_one_point_with_close_coordinate():
    grid = PointSkeleton(lon=(10, 11), lat=(0, 5))
    yanked_points = grid.yank_point(lon=10, lat=0.01)
    assert len(yanked_points["inds"]) == 1
    assert len(yanked_points["dx"]) == 1
    assert yanked_points["inds"][0] == 0
    np.testing.assert_almost_equal(
        int(yanked_points["dx"][0]), int(distance_2points(0, 10, 0.01, 10))
    )


def test_yank_several_points_with_close_coordinates():
    grid = PointSkeleton(lon=(10, 11, 12, 13, 14), lat=(0, 1, 2, 3, 3.5))
    yanked_points = grid.yank_point(
        lon=(10.001, 12, 13.01), lat=(0, 2.001, 3.001), fast=True
    )
    assert len(yanked_points["inds"]) == 3
    assert len(yanked_points["dx"]) == 3
    np.testing.assert_array_equal(yanked_points["inds"], np.array([0, 2, 3]))
    expected_dx = np.array(
        [
            distance_2points(0, 10, 0, 10.001),
            distance_2points(2, 12, 2.001, 12),
            distance_2points(3.0, 13, 3.001, 13.01),
        ]
    )
    np.testing.assert_array_almost_equal(
        (0.1 * yanked_points["dx"]).astype(int), (0.1 * expected_dx).astype(int)
    )


def test_yank_cartesian_point_from_spherical_grid():
    data = PointSkeleton(lon=(9.0, 9.1, 11.0), lat=(60.0, 60.9, 61.0))
    data.utm.set((33, "N"))

    dd = data.yank_point(x=165640, y=6666593)
    assert dd["inds"][0] == 0
    assert dd["dx"][0] < 1

    dd = data.yank_point(x=283749, y=6769393)
    assert dd["inds"][0] == 2
    assert dd["dx"][0] < 1
