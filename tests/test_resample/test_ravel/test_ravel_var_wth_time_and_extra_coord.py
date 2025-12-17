from geo_skeletons import PointSkeleton, GriddedSkeleton
import pytest
import numpy as np

def test_ravel_lonlat():
    grid = GriddedSkeleton.add_time().add_coord('member').add_datavar('hs')(lon=(10,20,30), lat=(50,60,70), time=('2020-01-01 00:00', '2020-01-01 01:00'), member=[0,1,2])
    data = np.array([[1,2,3],[4,5,6],[7,8,9]])
    data = np.array([data,data*10])
    data = np.array([data,data*1.1, data*1.2])
    grid.set_hs(data, coords=['lat', 'time', 'lon','member'])
    
    points = grid.ravel()
    glon, glat = grid.lonlat()
    lon, lat = points.lonlat()
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)
    np.testing.assert_array_almost_equal(grid.hs().ravel(), points.hs().ravel())


def test_ravel_xy():
    grid = GriddedSkeleton.add_time().add_coord('member').add_datavar('hs')(x=(10,20,30), y=(50,60,70), time=('2020-01-01 00:00', '2020-01-01 01:00'), member=[0,1,2])
    data = np.array([[1,2,3],[4,5,6],[7,8,9]])
    data = np.array([data,data*10])
    data = np.array([data,data*1.1, data*1.2])
    grid.set_hs(data, coords=['y', 'time', 'x','member'])
    points = grid.ravel()
    glon, glat = grid.xy()
    lon, lat = points.xy()
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)
    np.testing.assert_array_almost_equal(grid.hs().ravel(), points.hs().ravel())


def test_ravel_lonlat_and_reproj():
    grid = GriddedSkeleton.add_time().add_coord('member').add_datavar('hs')(lon=(10,20,30), lat=(50,60,70), time=('2020-01-01 00:00', '2020-01-01 01:00'), member=[0,1,2])
    data = np.array([[1,2,3],[4,5,6],[7,8,9]])
    data = np.array([data,data*10])
    data = np.array([data,data*1.1, data*1.2])
    grid.set_hs(data, coords=['lat', 'time', 'lon','member'])
    points = grid.ravel(proj='xy')
    glon, glat = grid.xy()
    lon, lat = points.xy(native=True)
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)
    np.testing.assert_array_almost_equal(grid.hs().ravel(), points.hs().ravel())


def test_ravel_xy_and_reporj():
    grid = GriddedSkeleton.add_time().add_coord('member').add_datavar('hs')(x=(10,20,30), y=(50,60,70), time=('2020-01-01 00:00', '2020-01-01 01:00'), member=[0,1,2])
    data = np.array([[1,2,3],[4,5,6],[7,8,9]])
    data = np.array([data,data*10])
    data = np.array([data,data*1.1, data*1.2])
    grid.set_hs(data, coords=['y', 'time', 'x','member'])
    grid.proj.set((33,'W'))
    points = grid.ravel(proj='lonlat')
    glon, glat = grid.lonlat()
    lon, lat = points.xy(native=True)
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)
    np.testing.assert_array_almost_equal(grid.hs().ravel(), points.hs().ravel())