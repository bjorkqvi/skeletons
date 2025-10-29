from geo_skeletons import PointSkeleton
import numpy as np
import pytest


def test_mask_wrong_shape():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))
    mask = np.full((4, 2), True)

    with pytest.raises(ValueError):
        points.lon(mask=mask)

    with pytest.raises(ValueError):
        points.lat(mask=mask)

    with pytest.raises(ValueError):
        points.x(mask=mask)

    with pytest.raises(ValueError):
        points.y(mask=mask)


def test_trivial_mask():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))
    mask = np.full((4,), True)

    np.testing.assert_array_almost_equal(points.lon(), points.lon(mask=mask))
    np.testing.assert_array_almost_equal(points.lat(), points.lat(mask=mask))


def test_drop_one_point():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))
    mask = np.full((4,), True)
    mask[-1] = False

    np.testing.assert_array_almost_equal(points.lon()[:-1], points.lon(mask=mask))
    np.testing.assert_array_almost_equal(points.lat()[:-1], points.lat(mask=mask))

    points2 = PointSkeleton(x=points.x(), y=points.y())
    np.testing.assert_array_almost_equal(points2.x()[:-1], points2.x(mask=mask))
    np.testing.assert_array_almost_equal(points2.y()[:-1], points2.y(mask=mask))


def test_drop_one_point_gridmethod():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))
    mask = np.full((4,), True)
    mask[-1] = False

    np.testing.assert_array_almost_equal(points.lon()[:-1], points.longrid(mask=mask))
    np.testing.assert_array_almost_equal(points.lat()[:-1], points.latgrid(mask=mask))

    points2 = PointSkeleton(x=points.x(), y=points.y())
    np.testing.assert_array_almost_equal(points2.x()[:-1], points2.xgrid(mask=mask))
    np.testing.assert_array_almost_equal(points2.y()[:-1], points2.ygrid(mask=mask))


def test_drop_one_point_lonlat_method():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(6, 7, 8, 9))
    mask = np.full((4,), True)
    mask[-1] = False

    lon, lat = points.lonlat(mask=mask)
    np.testing.assert_array_almost_equal(points.lon()[:-1], lon)
    np.testing.assert_array_almost_equal(points.lat()[:-1], lat)

    points2 = PointSkeleton(x=points.x(), y=points.y())
    x, y = points.xy(mask=mask)
    np.testing.assert_array_almost_equal(points2.x()[:-1], x)
    np.testing.assert_array_almost_equal(points2.y()[:-1], y)
