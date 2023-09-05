from ..point_skeleton import PointSkeleton
import numpy as np


def test_yank_one_point_with_exact_coordinate():
    grid = PointSkeleton(x=(1, 2), y=(0, 3))
    yanked_points = grid.yank_point(x=1, y=0)
    assert len(yanked_points["inds"]) == 1
    assert len(yanked_points["dx"]) == 1
    assert yanked_points["inds"][0] == 0
    np.testing.assert_almost_equal(yanked_points["dx"][0], 0)


def test_yank_several_points_with_exact_coordinates():
    grid = PointSkeleton(x=(1, 2, 3, 4, 5), y=(10, 20, 30, 40, 50))
    yanked_points = grid.yank_point(x=(1, 3, 5), y=(10, 30, 50))
    assert len(yanked_points["inds"]) == 3
    assert len(yanked_points["dx"]) == 3
    np.testing.assert_array_equal(yanked_points["inds"], np.array([0, 2, 4]))
    np.testing.assert_array_almost_equal(yanked_points["dx"], np.array([0, 0, 0]))


def test_yank_one_point_with_close_coordinate():
    grid = PointSkeleton(x=(1, 2), y=(0, 3))
    yanked_points = grid.yank_point(x=1, y=0.1)
    assert len(yanked_points["inds"]) == 1
    assert len(yanked_points["dx"]) == 1
    assert yanked_points["inds"][0] == 0
    np.testing.assert_almost_equal(yanked_points["dx"][0], 0.1)


def test_yank_several_points_with_close_coordinates():
    grid = PointSkeleton(x=(1, 2, 3, 4, 5), y=(10, 20, 30, 40, 50))
    yanked_points = grid.yank_point(x=(1.1, 3, 5.1), y=(10, 30.5, 50.1))
    assert len(yanked_points["inds"]) == 3
    assert len(yanked_points["dx"]) == 3
    np.testing.assert_array_equal(yanked_points["inds"], np.array([0, 2, 4]))
    np.testing.assert_array_almost_equal(
        yanked_points["dx"], np.array([0.1, 0.5, (0.1**2 + 0.1**2) ** 0.5])
    )
