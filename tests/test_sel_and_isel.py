from geo_skeletons import PointSkeleton, GriddedSkeleton
import numpy as np


def test_point():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))

    np.testing.assert_array_almost_equal(
        points.lon(), points.sel(inds=slice(0, 10)).lon()
    )

    np.testing.assert_array_almost_equal([3, 4], points.sel(inds=slice(2, 4)).lon())
    np.testing.assert_array_almost_equal([1, 4], points.sel(inds=[0, 3]).lon())

    np.testing.assert_array_almost_equal([10, 40], points.sel(inds=[0, 3]).lat())

    np.testing.assert_array_almost_equal(
        points.lon(), points.isel(inds=slice(0, 10)).lon()
    )

    np.testing.assert_array_almost_equal([3, 4], points.isel(inds=slice(2, 4)).lon())
    np.testing.assert_array_almost_equal([1, 4], points.isel(inds=[0, 3]).lon())

    np.testing.assert_array_almost_equal([10, 40], points.isel(inds=[0, 3]).lat())


def test_gridded():
    points = GriddedSkeleton(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))
    np.testing.assert_array_almost_equal(
        points.lon(), points.sel(lon=slice(0, 10)).lon()
    )
    np.testing.assert_array_almost_equal(points.lon(), points.sel(lat=10).lon())
    np.testing.assert_array_almost_equal([1, 2, 3], points.sel(lon=slice(1, 3)).lon())

    np.testing.assert_array_almost_equal(
        [1, 2, 3], points.sel(lon=slice(1, 3), lat=10).lon()
    )

    np.testing.assert_array_almost_equal([1, 2, 4], points.sel(lon=[1, 2, 4]).lon())

    np.testing.assert_array_almost_equal([1, 2, 4], points.isel(lon=[0, 1, 3]).lon())
    np.testing.assert_array_almost_equal(points.lat(), points.isel(lon=[0, 1, 3]).lat())

    np.testing.assert_array_almost_equal([1, 2, 4], points.isel(lon=[0, 0, 1, 3]).lon())
