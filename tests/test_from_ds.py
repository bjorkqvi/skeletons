from geo_skeletons.point_skeleton import PointSkeleton
from geo_skeletons.gridded_skeleton import GriddedSkeleton
from geo_skeletons.decorators import add_coord, add_datavar
import numpy as np


def test_point_cartesian():
    @add_coord(name="test")
    class AddedCoordinate(PointSkeleton):
        pass

    @add_coord(name="test2")
    class AnotherAddedCoordinate(PointSkeleton):
        pass

    grid = PointSkeleton(x=(1, 2), y=(0, 3))
    grid2 = AddedCoordinate.from_ds(grid.ds(), test=np.arange(2))
    np.testing.assert_array_almost_equal(grid2.x(), np.array([1, 2]))
    np.testing.assert_array_almost_equal(grid2.y(), np.array([0, 3]))
    np.testing.assert_array_almost_equal(grid2.inds(), np.array([0, 1]))
    np.testing.assert_array_almost_equal(grid2.test(), np.array([0, 1]))
    assert list(grid2.ds().coords) == ["inds", "test"]

    grid3 = AnotherAddedCoordinate.from_ds(grid2.ds(), test2=np.array([5, 6]))
    np.testing.assert_array_almost_equal(grid3.x(), np.array([1, 2]))
    np.testing.assert_array_almost_equal(grid3.y(), np.array([0, 3]))
    np.testing.assert_array_almost_equal(grid3.inds(), np.array([0, 1]))
    np.testing.assert_array_almost_equal(grid3.test2(), np.array([5, 6]))
    assert list(grid3.ds().coords) == ["inds", "test2"]


def test_point_spherical():
    @add_coord(name="test")
    class AddedCoordinate(PointSkeleton):
        pass

    grid = PointSkeleton(lon=(1, 2), lat=(0, 3))
    grid2 = AddedCoordinate.from_ds(grid.ds(), test=np.arange(2))
    np.testing.assert_array_almost_equal(grid2.lon(), np.array([1, 2]))
    np.testing.assert_array_almost_equal(grid2.lat(), np.array([0, 3]))
    np.testing.assert_array_almost_equal(grid2.inds(), np.array([0, 1]))
    np.testing.assert_array_almost_equal(grid2.test(), np.array([0, 1]))
    assert list(grid2.ds().coords) == ["inds", "test"]


def test_gridded_cartesian():
    @add_coord(name="test")
    class AddedCoordinate(GriddedSkeleton):
        pass

    grid = GriddedSkeleton(x=(1, 4), y=(0, 4))
    grid.set_spacing(nx=4, ny=5)
    grid2 = AddedCoordinate.from_ds(grid.ds(), test=np.arange(2))
    grid2.set_test_spacing(nx=3)
    np.testing.assert_array_almost_equal(grid2.x(), np.arange(4) + 1)
    np.testing.assert_array_almost_equal(grid2.y(), np.arange(5))
    np.testing.assert_array_almost_equal(grid2.test(), np.array([0, 0.5, 1]))
    assert set(grid2.ds().coords) == set(["x", "y", "test"])


def test_gridded_spherical():
    @add_coord(name="test")
    class AddedCoordinate(GriddedSkeleton):
        pass

    grid = GriddedSkeleton(lon=(1, 4), lat=(0, 4))
    grid.set_spacing(nx=4, ny=5)
    grid2 = AddedCoordinate.from_ds(grid.ds(), test=np.arange(2))
    grid2.set_test_spacing(nx=3)
    np.testing.assert_array_almost_equal(grid2.lon(), np.arange(4) + 1)
    np.testing.assert_array_almost_equal(grid2.lat(), np.arange(5))
    np.testing.assert_array_almost_equal(grid2.test(), np.array([0, 0.5, 1]))
    assert set(grid2.ds().coords) == set(["lon", "lat", "test"])
