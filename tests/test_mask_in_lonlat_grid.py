from geo_skeletons import GriddedSkeleton
import numpy as np
import pytest


def test_mask_wrong_shape():
    points = GriddedSkeleton(lon=(1, 2, 3, 4, 5), lat=(6, 7, 8, 9))
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
    points = GriddedSkeleton(lon=(1, 2, 3, 4, 5), lat=(6, 7, 8, 9))
    mask = np.full((4, 5), True)

    np.testing.assert_array_almost_equal(points.lon(), points.lon(mask=mask))
    np.testing.assert_array_almost_equal(points.lat(), points.lat(mask=mask))


def test_drop_one_point():
    points = GriddedSkeleton(lon=(1, 2, 3, 4, 5), lat=(6, 7, 8, 9))
    mask = np.full((4, 5), True)
    mask[-1, -1] = False

    # This does nothing, since all longitudes and latitudes are still neede to describe all true points
    np.testing.assert_array_almost_equal(points.lon(), points.lon(mask=mask))
    np.testing.assert_array_almost_equal(points.lat(), points.lat(mask=mask))

    points2 = GriddedSkeleton(x=points.x(), y=points.y())
    np.testing.assert_array_almost_equal(points2.x(), points2.x(mask=mask))
    np.testing.assert_array_almost_equal(points2.y(), points2.y(mask=mask))


def test_drop_one_row():
    points = GriddedSkeleton(lon=(1, 2, 3, 4, 5), lat=(6, 7, 8, 9))
    mask = np.full((4, 5), True)
    mask[-1, :] = False
    np.testing.assert_array_almost_equal(points.lon(), points.lon(mask=mask))
    np.testing.assert_array_almost_equal(points.lat()[:-1], points.lat(mask=mask))

    points2 = GriddedSkeleton(x=points.x(), y=points.y())
    np.testing.assert_array_almost_equal(points2.x(), points2.x(mask=mask))
    np.testing.assert_array_almost_equal(points2.y()[:-1], points2.y(mask=mask))


def test_drop_one_column():
    points = GriddedSkeleton(lon=(1, 2, 3, 4, 5), lat=(6, 7, 8, 9))
    mask = np.full((4, 5), True)
    mask[:, -1] = False
    np.testing.assert_array_almost_equal(points.lon()[:-1], points.lon(mask=mask))
    np.testing.assert_array_almost_equal(points.lat(), points.lat(mask=mask))

    points2 = GriddedSkeleton(x=points.x(), y=points.y())
    np.testing.assert_array_almost_equal(points2.x()[:-1], points2.x(mask=mask))
    np.testing.assert_array_almost_equal(points2.y(), points2.y(mask=mask))


def test_drop_one_row_lonlatmethod():
    points = GriddedSkeleton(lon=(1, 2, 3, 4, 5), lat=(6, 7, 8, 9))
    mask = np.full((4, 5), True)
    mask[-1, :] = False

    lon, lat = points.lonlat(mask=mask.ravel())
    lons = (1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
    lats = (6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8)
    np.testing.assert_array_almost_equal(lons, lon)
    np.testing.assert_array_almost_equal(lats, lat)

    points2 = GriddedSkeleton(x=points.x(), y=points.y())
    x, y = points2.xy(mask=mask)
    xs = np.tile(points2.x(), 3)
    ys = np.repeat(points2.y()[:-1], 5)

    np.testing.assert_array_almost_equal(x, xs)
    np.testing.assert_array_almost_equal(y, ys)
