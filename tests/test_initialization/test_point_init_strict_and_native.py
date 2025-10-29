from geo_skeletons.point_skeleton import PointSkeleton
import numpy as np


def test_spherical_strict():
    points = PointSkeleton(lon=(1, 2), lat=(0, 3))

    assert points.x(strict=True) is None
    assert points.y(strict=True) is None

    np.testing.assert_array_almost_equal(points.lon(strict=True), np.array([1, 2]))
    np.testing.assert_array_almost_equal(points.lat(strict=True), np.array([0, 3]))

    np.testing.assert_array_almost_equal(
        points.lonlat(strict=True), (np.array([1, 2]), np.array([0, 3]))
    )

    assert points.xy(strict=True) == (None, None)


def test_spherical_native():
    points = PointSkeleton(lon=(1, 2), lat=(0, 3))

    np.testing.assert_array_almost_equal(points.lon(native=True), np.array([1, 2]))
    np.testing.assert_array_almost_equal(points.lat(native=True), np.array([0, 3]))
    np.testing.assert_array_almost_equal(points.x(native=True), np.array([1, 2]))
    np.testing.assert_array_almost_equal(points.y(native=True), np.array([0, 3]))

    np.testing.assert_array_almost_equal(
        points.lonlat(native=True), (np.array([1, 2]), np.array([0, 3]))
    )
    np.testing.assert_array_almost_equal(
        points.xy(native=True), (np.array([1, 2]), np.array([0, 3]))
    )


def test_cartesian_strict():
    points = PointSkeleton(x=(1, 2), y=(0, 3))

    assert points.lon(strict=True) is None
    assert points.lat(strict=True) is None

    np.testing.assert_array_almost_equal(points.x(strict=True), np.array([1, 2]))
    np.testing.assert_array_almost_equal(points.y(strict=True), np.array([0, 3]))

    np.testing.assert_array_almost_equal(
        points.xy(strict=True), (np.array([1, 2]), np.array([0, 3]))
    )

    assert points.lonlat(strict=True) == (None, None)


def test_cartesian_native():
    points = PointSkeleton(x=(1, 2), y=(0, 3))

    np.testing.assert_array_almost_equal(points.lon(native=True), np.array([1, 2]))
    np.testing.assert_array_almost_equal(points.lat(native=True), np.array([0, 3]))
    np.testing.assert_array_almost_equal(points.x(native=True), np.array([1, 2]))
    np.testing.assert_array_almost_equal(points.y(native=True), np.array([0, 3]))

    np.testing.assert_array_almost_equal(
        points.lonlat(native=True), (np.array([1, 2]), np.array([0, 3]))
    )
    np.testing.assert_array_almost_equal(
        points.xy(native=True), (np.array([1, 2]), np.array([0, 3]))
    )
