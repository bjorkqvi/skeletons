from geo_skeletons.point_skeleton import PointSkeleton
from geo_skeletons.gridded_skeleton import GriddedSkeleton
from geo_skeletons.decorators import add_coord, add_datavar
import numpy as np


def test_absorb_point_cartesian():
    grid = PointSkeleton(x=(1, 2), y=(0, 3))
    grid2 = PointSkeleton(x=(3, 4), y=(0, 3))
    grid = grid.absorb(grid2, dim="inds")
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
    grid = grid.absorb(grid2, dim="inds")

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
    grid = grid.absorb(grid2, dim="x")
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
    grid = grid.absorb(grid2, dim="lon")
    assert grid.inds() is None
    np.testing.assert_array_almost_equal(grid.lon(), np.array([1, 2, 3, 4]))
    np.testing.assert_array_almost_equal(grid.lat(), np.array([0, 3]))
    np.testing.assert_array_almost_equal(
        grid.lonlat(),
        (np.array([1, 2, 3, 4, 1, 2, 3, 4]), np.array([0, 0, 0, 0, 3, 3, 3, 3])),
    )


def test_absorb_point_skeleton_along_new_coordinate():
    @add_datavar(name="test")
    @add_coord(name="z")
    class TestSkeleton(PointSkeleton):
        pass

    points1 = TestSkeleton(x=(1, 2), y=(3, 4), z=(0, 1, 2, 3, 4))
    data1 = np.full(points1.size(), 1)
    points1.set_test(data1)

    points2 = TestSkeleton(x=(1, 2), y=(3, 4), z=(5, 6, 7))
    data2 = np.full(points2.size(), 2)
    points2.set_test(data2)
    points3 = points1.absorb(points2, dim="z")
    np.testing.assert_array_almost_equal(points3.z(), np.arange(8))
    assert np.all(points3.test(z=slice(0, 4)) == points1.test())
    assert np.all(points3.test(z=slice(5, 7)) == points2.test())
    assert points3.size() == (2, 8)


def test_absorb_gridded_skeleton_along_new_coordinate():
    @add_datavar(name="test")
    @add_coord(name="z")
    class TestSkeleton(GriddedSkeleton):
        pass

    points1 = TestSkeleton(x=(1, 2), y=(3, 4), z=(0, 1, 2, 3, 4))
    points1.set_spacing(nx=3, ny=4)
    data1 = np.full(points1.size(), 1)
    points1.set_test(data1)

    points2 = TestSkeleton(x=(1, 2), y=(3, 4), z=(5, 6, 7))
    points2.set_spacing(nx=3, ny=4)
    data2 = np.full(points2.size(), 2)
    points2.set_test(data2)

    points3 = points1.absorb(points2, dim="z")

    np.testing.assert_array_almost_equal(points3.z(), np.arange(8))
    assert np.all(points3.test(z=slice(0, 4)) == points1.test())
    assert np.all(points3.test(z=slice(5, 7)) == points2.test())
    assert points3.size() == (4, 3, 8)
