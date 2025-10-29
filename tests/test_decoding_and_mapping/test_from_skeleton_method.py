from geo_skeletons import PointSkeleton, GriddedSkeleton
import numpy as np
import pytest


def test_point_from_point():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))

    new_points = PointSkeleton.from_skeleton(points)
    np.testing.assert_array_almost_equal(points.lon(), new_points.lon())
    np.testing.assert_array_almost_equal(points.lat(), new_points.lat())


def test_point_from_point_mask():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))

    new_points = PointSkeleton.from_skeleton(points, mask=[True, True, False, True])
    np.testing.assert_array_almost_equal([1, 2, 4], new_points.lon())
    np.testing.assert_array_almost_equal([6, 7, 9], new_points.lat())


def test_point_from_gridded():
    grid = GriddedSkeleton(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))

    points = PointSkeleton.from_skeleton(grid)

    np.testing.assert_array_almost_equal(grid.longrid().ravel(), points.lon())
    np.testing.assert_array_almost_equal(grid.latgrid().ravel(), points.lat())


def test_point_from_gridded_mask():
    grid = GriddedSkeleton(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))
    mask = np.array(
        [
            [True, True, True, True],
            [False, False, False, False],
            [False, False, False, False],
            [True, True, True, False],
        ]
    )
    new_lon = [1, 2, 3, 4, 1, 2, 3]
    new_lat = [6, 6, 6, 6, 9, 9, 9]
    points = PointSkeleton.from_skeleton(grid, mask=mask)

    np.testing.assert_array_almost_equal(new_lon, points.lon())
    np.testing.assert_array_almost_equal(new_lat, points.lat())


def test_gridded_from_gridded():
    grid = GriddedSkeleton(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))
    new_grid = GriddedSkeleton.from_skeleton(grid)
    np.testing.assert_array_almost_equal(grid.lon(), new_grid.lon())
    np.testing.assert_array_almost_equal(grid.lat(), new_grid.lat())


def test_gridded_from_gridded_mask():
    grid = GriddedSkeleton(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))
    mask = np.array(
        [
            [True, True, True, True],
            [False, False, False, False],
            [False, False, False, False],
            [True, True, True, True],
        ]
    )
    new_grid = GriddedSkeleton.from_skeleton(grid, mask=mask)
    np.testing.assert_array_almost_equal(new_grid.lon(), [1, 2, 3, 4])
    np.testing.assert_array_almost_equal(new_grid.lat(), [6, 9])


def test_gridded_from_point():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))

    with pytest.raises(TypeError):
        grid = GriddedSkeleton.from_skeleton(points)
