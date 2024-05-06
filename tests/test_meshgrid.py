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

    assert points.longrid() is None
    assert points.latgrid() is None


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


def test_gridded_cartesian():
    x = (1, 2, 3, 4, 5)
    y = (6, 7, 8, 9)
    points = GriddedSkeleton(x=x, y=y)

    xgrid = points.xgrid()
    np.testing.assert_almost_equal(xgrid[0, :], x)
    np.testing.assert_almost_equal(xgrid[-1, :], x)
    assert xgrid.shape == (4, 5)

    ygrid = points.ygrid()
    np.testing.assert_almost_equal(ygrid[:, 0], y)
    np.testing.assert_almost_equal(ygrid[:, -1], y)
    assert ygrid.shape == (4, 5)
    longrid = points.longrid()
    assert longrid is None  # No UTM zone

    latgrid = points.latgrid()
    assert latgrid is None  # No UTM zone

    assert points.longrid(strict=True) is None
    assert points.latgrid(strict=True) is None

    np.testing.assert_almost_equal(points.xgrid(), points.xgrid(strict=True))
    np.testing.assert_almost_equal(points.xgrid(), points.xgrid(native=True))

    np.testing.assert_almost_equal(points.ygrid(), points.ygrid(strict=True))
    np.testing.assert_almost_equal(points.ygrid(), points.ygrid(native=True))


def test_gridded_spherical():
    lon = (5, 6, 7, 8)
    lat = (60, 61, 62, 63, 64)
    points = GriddedSkeleton(lon=lon, lat=lat)

    longrid = points.longrid()
    np.testing.assert_almost_equal(longrid[0, :], lon)
    np.testing.assert_almost_equal(longrid[-1, :], lon)
    assert longrid.shape == (5, 4)

    latgrid = points.latgrid()
    np.testing.assert_almost_equal(latgrid[:, 0], lat)
    np.testing.assert_almost_equal(latgrid[:, -1], lat)
    assert latgrid.shape == (5, 4)

    x, y = points.xy()
    np.testing.assert_almost_equal(points.xgrid().ravel(), x)
    assert points.xgrid().shape == (5, 4)

    np.testing.assert_almost_equal(points.ygrid().ravel(), y)
    assert points.ygrid().shape == (5, 4)

    assert points.xgrid(strict=True) is None
    assert points.ygrid(strict=True) is None

    np.testing.assert_almost_equal(points.longrid(), points.longrid(strict=True))
    np.testing.assert_almost_equal(points.longrid(), points.longrid(native=True))

    np.testing.assert_almost_equal(points.latgrid(), points.latgrid(strict=True))
    np.testing.assert_almost_equal(points.latgrid(), points.latgrid(native=True))
