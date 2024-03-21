from geo_skeletons.gridded_skeleton import GriddedSkeleton
import numpy as np


def test_spherical_strict():
    grid = GriddedSkeleton(lon=(1, 2), lat=(0, 3))

    assert grid.x(strict=True) is None
    assert grid.y(strict=True) is None

    np.testing.assert_array_almost_equal(grid.lon(strict=True), np.array([1, 2]))
    np.testing.assert_array_almost_equal(grid.lat(strict=True), np.array([0, 3]))

    np.testing.assert_array_almost_equal(
        grid.lonlat(strict=True), (np.array([1, 2, 1, 2]), np.array([0, 0, 3, 3]))
    )

    assert grid.xy(strict=True) == (None, None)


def test_spherical_native():
    grid = GriddedSkeleton(lon=(1, 2), lat=(0, 3))

    np.testing.assert_array_almost_equal(grid.lon(native=True), np.array([1, 2]))
    np.testing.assert_array_almost_equal(grid.lat(native=True), np.array([0, 3]))
    np.testing.assert_array_almost_equal(grid.x(native=True), np.array([1, 2]))
    np.testing.assert_array_almost_equal(grid.y(native=True), np.array([0, 3]))

    np.testing.assert_array_almost_equal(
        grid.lonlat(native=True), (np.array([1, 2, 1, 2]), np.array([0, 0, 3, 3]))
    )
    np.testing.assert_array_almost_equal(
        grid.xy(native=True), (np.array([1, 2, 1, 2]), np.array([0, 0, 3, 3]))
    )


def test_cartesian_strict():
    grid = GriddedSkeleton(x=(1, 2), y=(0, 3))

    assert grid.lon(strict=True) is None
    assert grid.lat(strict=True) is None

    np.testing.assert_array_almost_equal(grid.x(strict=True), np.array([1, 2]))
    np.testing.assert_array_almost_equal(grid.y(strict=True), np.array([0, 3]))

    np.testing.assert_array_almost_equal(
        grid.xy(strict=True), (np.array([1, 2, 1, 2]), np.array([0, 0, 3, 3]))
    )

    assert grid.lonlat(strict=True) == (None, None)


def test_cartesian_native():
    grid = GriddedSkeleton(x=(1, 2), y=(0, 3))

    np.testing.assert_array_almost_equal(grid.lon(native=True), np.array([1, 2]))
    np.testing.assert_array_almost_equal(grid.lat(native=True), np.array([0, 3]))
    np.testing.assert_array_almost_equal(grid.x(native=True), np.array([1, 2]))
    np.testing.assert_array_almost_equal(grid.y(native=True), np.array([0, 3]))

    np.testing.assert_array_almost_equal(
        grid.lonlat(native=True), (np.array([1, 2, 1, 2]), np.array([0, 0, 3, 3]))
    )
    np.testing.assert_array_almost_equal(
        grid.xy(native=True), (np.array([1, 2, 1, 2]), np.array([0, 0, 3, 3]))
    )
