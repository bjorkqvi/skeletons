from geo_skeletons.point_skeleton import PointSkeleton
import numpy as np


def test_init_trivial():
    grid = PointSkeleton(x=(1, 2), y=(0, 3))
    repr = print(grid)
    assert grid.nx() == 2
    assert grid.ny() == 2
    assert grid.size() == (2,)
    np.testing.assert_array_almost_equal(grid.x(), np.array([1, 2]))
    np.testing.assert_array_almost_equal(grid.x(normalize=True), np.array([0, 1]))
    np.testing.assert_array_almost_equal(grid.y(), np.array([0, 3]))
    np.testing.assert_array_almost_equal(grid.y(normalize=True), np.array([0, 3]))
    np.testing.assert_array_almost_equal(grid.inds(), np.array([0, 1]))
    np.testing.assert_array_almost_equal(
        grid.xy(), (np.array([1, 2]), np.array([0, 3]))
    )


def test_init_one_point_in_y():
    grid = PointSkeleton(x=(3, 5), y=(0, 0))
    assert grid.nx() == 2
    assert grid.ny() == 2
    assert grid.size() == (2,)
    np.testing.assert_array_almost_equal(grid.x(), np.array([3, 5]))
    np.testing.assert_array_almost_equal(grid.y(), np.array([0, 0]))
    np.testing.assert_array_almost_equal(grid.inds(), np.array([0, 1]))
    np.testing.assert_array_almost_equal(
        grid.xy(), (np.array([3, 5]), np.array([0, 0]))
    )


def test_init_one_point_in_y_constant():
    grid = PointSkeleton(x=(3, 5), y=0)
    assert grid.nx() == 2
    assert grid.ny() == 2
    assert grid.size() == (2,)
    np.testing.assert_array_almost_equal(grid.x(), np.array([3, 5]))
    np.testing.assert_array_almost_equal(grid.y(), np.array([0, 0]))
    np.testing.assert_array_almost_equal(grid.inds(), np.array([0, 1]))
    np.testing.assert_array_almost_equal(
        grid.xy(), (np.array([3, 5]), np.array([0, 0]))
    )


def test_init_one_point_in_x():
    grid = PointSkeleton(x=(0, 0), y=(3, 5))
    assert grid.nx() == 2
    assert grid.ny() == 2
    assert grid.size() == (2,)
    np.testing.assert_array_almost_equal(grid.y(), np.array([3, 5]))
    np.testing.assert_array_almost_equal(grid.x(), np.array([0, 0]))
    np.testing.assert_array_almost_equal(grid.inds(), np.array([0, 1]))
    np.testing.assert_array_almost_equal(
        grid.xy(), (np.array([0, 0]), np.array([3, 5]))
    )


def test_init_one_point_in_x_constant():
    grid = PointSkeleton(x=0, y=(3, 5))
    assert grid.nx() == 2
    assert grid.ny() == 2
    assert grid.size() == (2,)
    np.testing.assert_array_almost_equal(grid.y(), np.array([3, 5]))
    np.testing.assert_array_almost_equal(grid.y(normalize=True), np.array([0, 2]))
    np.testing.assert_array_almost_equal(grid.x(), np.array([0, 0]))
    np.testing.assert_array_almost_equal(grid.x(normalize=True), np.array([0, 0]))
    np.testing.assert_array_almost_equal(grid.inds(), np.array([0, 1]))
    np.testing.assert_array_almost_equal(
        grid.xy(), (np.array([0, 0]), np.array([3, 5]))
    )


def test_init_one_point():
    grid = PointSkeleton(x=0, y=3)
    assert grid.nx() == 1
    assert grid.ny() == 1
    assert grid.size() == (1,)
    np.testing.assert_array_almost_equal(grid.y(), np.array([3]))
    np.testing.assert_array_almost_equal(grid.x(), np.array([0]))
    np.testing.assert_array_almost_equal(grid.inds(), np.array([0]))
    np.testing.assert_array_almost_equal(grid.xy(), (np.array([0]), np.array([3])))


def test_init_long():
    grid = PointSkeleton(x=[0, 2, 4, 5, 6, 6], y=[3, 1, 2, 3, 4, 5])
    assert grid.nx() == 6
    assert grid.ny() == 6
    assert grid.size() == (6,)
    np.testing.assert_array_almost_equal(grid.y(), np.array([3, 1, 2, 3, 4, 5]))
    np.testing.assert_array_almost_equal(grid.x(), np.array([0, 2, 4, 5, 6, 6]))
    np.testing.assert_array_almost_equal(grid.inds(), np.array([0, 1, 2, 3, 4, 5]))
    np.testing.assert_array_almost_equal(
        grid.xy(), (np.array([0, 2, 4, 5, 6, 6]), np.array([3, 1, 2, 3, 4, 5]))
    )
