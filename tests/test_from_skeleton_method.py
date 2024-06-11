from geo_skeletons import PointSkeleton, GriddedSkeleton
import numpy as np


def test_point_from_point():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))

    new_points = PointSkeleton.from_skeleton(points)
    np.testing.assert_array_almost_equal(points.lon(), new_points.lon())
    np.testing.assert_array_almost_equal(points.lat(), new_points.lat())


def test_point_from_gridded():
    grid = GriddedSkeleton(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))

    points = PointSkeleton.from_skeleton(grid)

    np.testing.assert_array_almost_equal(grid.longrid().ravel(), points.lon())
    np.testing.assert_array_almost_equal(grid.latgrid().ravel(), points.lat())
