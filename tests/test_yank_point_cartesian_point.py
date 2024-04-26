from geo_skeletons.point_skeleton import PointSkeleton
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


def test_yank_spherical_point_from_cartesian_grid():
    data = PointSkeleton(x=(165640, 180189, 283749), y=(6666593, 6766055, 6769393))
    data.utm.set((33, "N"))

    assert np.round(data.lon()[0]) == 9
    assert np.round(data.lat()[0]) == 60
    dd = data.yank_point(lon=9, lat=60)
    assert dd["inds"][0] == 0
    assert dd["dx"][0] < 1

    assert np.round(data.lon()[2]) == 11
    assert np.round(data.lat()[2]) == 61
    dd = data.yank_point(lon=11, lat=61)
    assert dd["inds"][0] == 2
    assert dd["dx"][0] < 1
