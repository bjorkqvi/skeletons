from geo_skeletons import PointSkeleton, GriddedSkeleton
import pytest
import numpy as np

def test_ravel_lonlat():
    grid = GriddedSkeleton.add_time()(lon=(10,20,30), lat=(50,60,70), time=('2020-01-01 00:00', '2020-01-01 06:00'))
    points = grid.ravel()
    glon, glat = grid.lonlat()
    lon, lat = points.lonlat()
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)
    assert grid.time(datetime=False) == points.time(datetime=False)


def test_ravel_xy():
    grid = GriddedSkeleton.add_time()(lon=(10,20,30), lat=(50,60,70), time=('2020-01-01 00:00', '2020-01-01 06:00'))
    points = grid.ravel()
    glon, glat = grid.xy()
    lon, lat = points.xy()
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)
    assert grid.time(datetime=False) == points.time(datetime=False)

def test_ravel_lonlat_and_reproj():
    grid = GriddedSkeleton.add_time()(lon=(10,20,30), lat=(50,60,70), time=('2020-01-01 00:00', '2020-01-01 06:00'))
    points = grid.ravel(proj='xy')
    glon, glat = grid.xy()
    lon, lat = points.xy(native=True)
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)
    assert grid.time(datetime=False) == points.time(datetime=False)

def test_ravel_xy_and_reporj():
    grid = GriddedSkeleton.add_time()(lon=(10,20,30), lat=(50,60,70), time=('2020-01-01 00:00', '2020-01-01 06:00'))
    grid.proj.set((33,'W'))
    points = grid.ravel(proj='lonlat')
    glon, glat = grid.lonlat()
    lon, lat = points.xy(native=True)
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)
    assert grid.time(datetime=False) == points.time(datetime=False)