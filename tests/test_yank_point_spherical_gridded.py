from geo_skeletons.gridded_skeleton import GriddedSkeleton
from geo_skeletons.distance_funcs import distance_2points

import numpy as np


def test_yank_one_point_with_exact_coordinate():
    grid = GriddedSkeleton(lon=(10, 11), lat=(0, 1))
    grid.set_spacing(nx=10, ny=10)
    yanked_points = grid.yank_point(lon=10, lat=0)
    assert len(yanked_points["inds_x"]) == 1
    assert len(yanked_points["inds_y"]) == 1
    assert len(yanked_points["dx"]) == 1
    assert yanked_points["inds_x"][0] == 0
    assert yanked_points["inds_y"][0] == 0
    np.testing.assert_almost_equal(yanked_points["dx"][0], 0)


def test_yank_several_points_with_exact_coordinates():
    grid = GriddedSkeleton(lon=(10, 14), lat=(0, 4))
    grid.set_spacing(nx=5, ny=5)
    yanked_points = grid.yank_point(lon=(10, 12, 14), lat=(0, 2, 4), fast=True)
    assert len(yanked_points["inds_x"]) == 3
    assert len(yanked_points["inds_y"]) == 3
    assert len(yanked_points["dx"]) == 3
    np.testing.assert_array_equal(yanked_points["inds_x"], np.array([0, 2, 4]))
    np.testing.assert_array_equal(yanked_points["inds_y"], np.array([0, 2, 4]))
    np.testing.assert_array_almost_equal(yanked_points["dx"], np.array([0, 0, 0]))


def test_yank_one_point_with_close_coordinate():
    grid = GriddedSkeleton(lon=(10, 11), lat=(0, 5))
    grid.set_spacing(nx=10, ny=10)
    yanked_points = grid.yank_point(lon=10, lat=0.01)
    assert len(yanked_points["inds_x"]) == 1
    assert len(yanked_points["inds_y"]) == 1
    assert len(yanked_points["dx"]) == 1
    assert yanked_points["inds_x"][0] == 0
    assert yanked_points["inds_y"][0] == 0
    np.testing.assert_almost_equal(
        int(yanked_points["dx"][0]), int(distance_2points(0, 10, 0.01, 10))
    )


def test_yank_several_points_with_close_coordinates():
    grid = GriddedSkeleton(lon=(10, 14), lat=(0, 4))
    grid.set_spacing(nx=5, ny=5)
    yanked_points = grid.yank_point(
        lon=(10.001, 12, 13.01), lat=(0, 2.001, 3.001), fast=True
    )
    assert len(yanked_points["inds_x"]) == 3
    assert len(yanked_points["inds_y"]) == 3
    assert len(yanked_points["dx"]) == 3
    np.testing.assert_array_equal(yanked_points["inds_x"], np.array([0, 2, 3]))
    np.testing.assert_array_equal(yanked_points["inds_y"], np.array([0, 2, 3]))
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
