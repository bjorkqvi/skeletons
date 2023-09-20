from geo_skeletons.gridded_skeleton import GriddedSkeleton
import numpy as np


def test_init_name():
    grid = GriddedSkeleton(x=(1, 2), y=(0, 3))
    assert grid.name == "LonelySkeleton"
    grid.name = "TestGrid"
    assert grid.name == "TestGrid"


def test_init_trivial():
    grid = GriddedSkeleton(x=(1, 2), y=(0, 3))
    repr = print(grid)
    assert grid.nx() == 2
    assert grid.ny() == 2
    assert grid.size() == (2, 2)
    np.testing.assert_array_almost_equal(grid.x(), np.array([1, 2]))
    np.testing.assert_array_almost_equal(grid.y(), np.array([0, 3]))
    assert grid.inds() is None
    np.testing.assert_array_almost_equal(
        grid.xy(), (np.array([1, 2, 1, 2]), np.array([0, 0, 3, 3]))
    )


def test_init_one_point_in_y():
    grid = GriddedSkeleton(x=(3, 5), y=(0, 0))
    assert grid.nx() == 2
    assert grid.ny() == 1
    assert grid.size() == (1, 2)
    np.testing.assert_array_almost_equal(grid.x(), np.array([3, 5]))
    np.testing.assert_array_almost_equal(grid.y(), np.array([0]))
    assert grid.inds() is None
    np.testing.assert_array_almost_equal(
        grid.xy(), (np.array([3, 5]), np.array([0, 0]))
    )


def test_init_one_point_in_y_constant():
    grid = GriddedSkeleton(x=(3, 5), y=0)
    repr = print(grid)
    assert grid.nx() == 2
    assert grid.ny() == 1
    assert grid.size() == (1, 2)
    np.testing.assert_array_almost_equal(grid.x(), np.array([3, 5]))
    np.testing.assert_array_almost_equal(grid.y(), np.array([0]))
    assert grid.inds() is None
    np.testing.assert_array_almost_equal(
        grid.xy(), (np.array([3, 5]), np.array([0, 0]))
    )


def test_init_one_point_in_x():
    grid = GriddedSkeleton(x=(0, 0), y=(3, 5))
    assert grid.nx() == 1
    assert grid.ny() == 2
    assert grid.size() == (2, 1)
    np.testing.assert_array_almost_equal(grid.y(), np.array([3, 5]))
    np.testing.assert_array_almost_equal(grid.x(), np.array([0]))
    assert grid.inds() is None
    np.testing.assert_array_almost_equal(
        grid.xy(), (np.array([0, 0]), np.array([3, 5]))
    )


def test_init_one_point_in_x_constant():
    grid = GriddedSkeleton(x=0, y=(3, 5))
    assert grid.nx() == 1
    assert grid.ny() == 2
    assert grid.size() == (2, 1)
    np.testing.assert_array_almost_equal(grid.y(), np.array([3, 5]))
    np.testing.assert_array_almost_equal(grid.x(), np.array([0]))
    assert grid.inds() is None
    np.testing.assert_array_almost_equal(
        grid.xy(), (np.array([0, 0]), np.array([3, 5]))
    )


def test_init_one_point():
    grid = GriddedSkeleton(x=0, y=3)
    assert grid.nx() == 1
    assert grid.ny() == 1
    assert grid.size() == (1, 1)
    np.testing.assert_array_almost_equal(grid.y(), np.array([3]))
    np.testing.assert_array_almost_equal(grid.x(), np.array([0]))
    assert grid.inds() is None
    np.testing.assert_array_almost_equal(grid.xy(), (np.array([0]), np.array([3])))
