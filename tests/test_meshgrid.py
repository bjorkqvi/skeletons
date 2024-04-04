from geo_skeletons import PointSkeleton, GriddedSkeleton
import numpy as np


def test_point_cartesian():
    x = (1, 2, 3, 4)
    y = (6, 7, 8, 9)
    points = PointSkeleton(x=x, y=y)
    np.testing.assert_almost_equal(points.xgrid(), x)
    np.testing.assert_almost_equal(points.ygrid(), y)

    np.testing.assert_almost_equal(points.xgrid(native=True), x)
    np.testing.assert_almost_equal(points.ygrid(native=True), y)

    np.testing.assert_almost_equal(points.xgrid(strict=True), x)
    np.testing.assert_almost_equal(points.ygrid(strict=True), y)

    np.testing.assert_almost_equal(points.longrid(native=True), x)
    np.testing.assert_almost_equal(points.latgrid(native=True), y)

    assert points.longrid(strict=True) is None
    assert points.latgrid(strict=True) is None

    np.testing.assert_almost_equal(points.longrid(), points.lon())
    np.testing.assert_almost_equal(points.latgrid(), points.lat())


def test_point_sphericalk():
    lon = (5, 6, 7, 8)
    lat = (60, 61, 62, 63)
    points = PointSkeleton(lon=lon, lat=lat)
    np.testing.assert_almost_equal(points.longrid(), lon)
    np.testing.assert_almost_equal(points.latgrid(), lat)

    np.testing.assert_almost_equal(points.longrid(native=True), lon)
    np.testing.assert_almost_equal(points.latgrid(native=True), lat)

    np.testing.assert_almost_equal(points.longrid(strict=True), lon)
    np.testing.assert_almost_equal(points.latgrid(strict=True), lat)

    np.testing.assert_almost_equal(points.xgrid(native=True), lon)
    np.testing.assert_almost_equal(points.ygrid(native=True), lat)

    assert points.xgrid(strict=True) is None
    assert points.ygrid(strict=True) is None

    np.testing.assert_almost_equal(points.xgrid(), points.x())
    np.testing.assert_almost_equal(points.ygrid(), points.y())
