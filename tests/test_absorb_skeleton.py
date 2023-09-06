from ..point_skeleton import PointSkeleton
from ..gridded_skeleton import GriddedSkeleton
import numpy as np


def test_absorb_point_cartesian():
    grid = PointSkeleton(x=(1, 2), y=(0, 3))
    grid2 = PointSkeleton(x=(3, 4), y=(0, 3))
    grid._absorb_skeleton(grid2, dimension="inds")

    np.testing.assert_array_almost_equal(grid.x(), np.array([1, 2, 3, 4]))
    np.testing.assert_array_almost_equal(grid.x(normalize=True), np.array([0, 1, 2, 3]))
    np.testing.assert_array_almost_equal(grid.y(), np.array([0, 3, 0, 3]))
    np.testing.assert_array_almost_equal(grid.y(normalize=True), np.array([0, 3, 0, 3]))
    np.testing.assert_array_almost_equal(grid.inds(), np.array([0, 1, 2, 3]))
    np.testing.assert_array_almost_equal(
        grid.xy(), (np.array([1, 2, 3, 4]), np.array([0, 3, 0, 3]))
    )


def test_absorb_point_spherical():
    grid = PointSkeleton(lon=(1, 2), lat=(0, 3))
    grid2 = PointSkeleton(lon=(3, 4), lat=(0, 3))
    grid._absorb_skeleton(grid2, dimension="inds")

    np.testing.assert_array_almost_equal(grid.lon(), np.array([1, 2, 3, 4]))
    np.testing.assert_array_almost_equal(grid.lat(), np.array([0, 3, 0, 3]))
    np.testing.assert_array_almost_equal(grid.inds(), np.array([0, 1, 2, 3]))
    np.testing.assert_array_almost_equal(
        grid.lonlat(), (np.array([1, 2, 3, 4]), np.array([0, 3, 0, 3]))
    )


def test_absorb_gridded_cartesian():
    grid = GriddedSkeleton(x=(1, 2), y=(0, 3))
    grid.set_spacing(nx=2, ny=2)
    grid2 = GriddedSkeleton(x=(3, 4), y=(0, 3))
    grid2.set_spacing(nx=2, ny=2)
    grid._absorb_skeleton(grid2, dimension="x")
    assert grid.inds() is None
    np.testing.assert_array_almost_equal(grid.x(), np.array([1, 2, 3, 4]))
    np.testing.assert_array_almost_equal(grid.x(normalize=True), np.array([0, 1, 2, 3]))
    np.testing.assert_array_almost_equal(grid.y(), np.array([0, 3]))
    np.testing.assert_array_almost_equal(grid.y(normalize=True), np.array([0, 3]))
    np.testing.assert_array_almost_equal(
        grid.xy(),
        (np.array([1, 2, 3, 4, 1, 2, 3, 4]), np.array([0, 0, 0, 0, 3, 3, 3, 3])),
    )


def test_absorb_gridded_spherical():
    grid = GriddedSkeleton(lon=(1, 2), lat=(0, 3))
    grid.set_spacing(nx=2, ny=2)
    grid2 = GriddedSkeleton(lon=(3, 4), lat=(0, 3))
    grid2.set_spacing(nx=2, ny=2)
    grid._absorb_skeleton(grid2, dimension="lon")
    assert grid.inds() is None
    np.testing.assert_array_almost_equal(grid.lon(), np.array([1, 2, 3, 4]))
    np.testing.assert_array_almost_equal(grid.lat(), np.array([0, 3]))
    np.testing.assert_array_almost_equal(
        grid.lonlat(),
        (np.array([1, 2, 3, 4, 1, 2, 3, 4]), np.array([0, 0, 0, 0, 3, 3, 3, 3])),
    )
