from geo_skeletons.gridded_skeleton import GriddedSkeleton
import numpy as np


def test_yank_one_point_with_exact_coordinate():
    grid = GriddedSkeleton(x=(1, 2), y=(0, 2))
    grid.set_spacing(nx=3, ny=3)
    yanked_points = grid.yank_point(x=1.5, y=1)
    assert len(yanked_points["inds_x"]) == 1
    assert len(yanked_points["inds_y"]) == 1
    assert len(yanked_points["dx"]) == 1
    assert yanked_points["inds_x"][0] == 1
    assert yanked_points["inds_y"][0] == 1
    np.testing.assert_almost_equal(yanked_points["dx"][0], 0)


def test_yank_several_points_with_exact_coordinates():
    grid = GriddedSkeleton(x=(1, 2), y=(10, 30))
    grid.set_spacing(nx=3, ny=3)
    yanked_points = grid.yank_point(x=(1, 1, 1.5), y=(10, 20, 30))
    assert len(yanked_points["inds_x"]) == 3
    assert len(yanked_points["inds_y"]) == 3
    assert len(yanked_points["dx"]) == 3
    np.testing.assert_array_equal(yanked_points["inds_x"], np.array([0, 0, 1]))
    np.testing.assert_array_equal(yanked_points["inds_y"], np.array([0, 1, 2]))
    np.testing.assert_array_almost_equal(yanked_points["dx"], np.array([0, 0, 0]))


def test_yank_one_point_with_close_coordinate():
    grid = GriddedSkeleton(x=(1, 2), y=(0, 2))
    grid.set_spacing(nx=3, ny=3)
    yanked_points = grid.yank_point(x=1.6, y=1)
    assert len(yanked_points["inds_x"]) == 1
    assert len(yanked_points["inds_y"]) == 1
    assert len(yanked_points["dx"]) == 1
    assert yanked_points["inds_x"][0] == 1
    assert yanked_points["inds_y"][0] == 1
    np.testing.assert_almost_equal(yanked_points["dx"][0], 0.1)


def test_yank_several_points_with_close_coordinates():
    grid = GriddedSkeleton(x=(1, 2), y=(10, 30))
    grid.set_spacing(nx=3, ny=3)
    yanked_points = grid.yank_point(x=(1, 0.9, 1.6), y=(11, 20, 31))
    assert len(yanked_points["inds_x"]) == 3
    assert len(yanked_points["inds_y"]) == 3
    assert len(yanked_points["dx"]) == 3
    np.testing.assert_array_equal(yanked_points["inds_x"], np.array([0, 0, 1]))
    np.testing.assert_array_equal(yanked_points["inds_y"], np.array([0, 1, 2]))
    np.testing.assert_array_almost_equal(
        yanked_points["dx"], np.array([1, 0.1, (0.1**2 + 1**2) ** 0.5])
    )
