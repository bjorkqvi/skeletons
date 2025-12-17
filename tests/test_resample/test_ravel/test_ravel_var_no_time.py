from geo_skeletons import PointSkeleton, GriddedSkeleton
import pytest
import numpy as np

def test_ravel_lonlat():
    grid = GriddedSkeleton.add_datavar('hs')(lon=(10,20,30), lat=(50,60,70))
    data = np.array([[1,2,3],[4,5,6],[7,8,9]])
    grid.set_hs(data)
    points = grid.ravel()
    glon, glat = grid.lonlat()
    lon, lat = points.lonlat()
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)
    np.testing.assert_array_almost_equal(grid.hs().ravel(), points.hs())


def test_ravel_xy():
    grid = GriddedSkeleton.add_datavar('hs')(lon=(10,20,30), lat=(50,60,70))
    data = np.array([[1,2,3],[4,5,6],[7,8,9]])
    grid.set_hs(data)
    points = grid.ravel()
    glon, glat = grid.xy()
    lon, lat = points.xy()
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)
    np.testing.assert_array_almost_equal(grid.hs().ravel(), points.hs())


def test_ravel_lonlat_and_reproj():
    grid = GriddedSkeleton.add_datavar('hs')(lon=(10,20,30), lat=(50,60,70))
    data = np.array([[1,2,3],[4,5,6],[7,8,9]])
    grid.set_hs(data)
    points = grid.ravel(proj='xy')
    glon, glat = grid.xy()
    lon, lat = points.xy(native=True)
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)
    np.testing.assert_array_almost_equal(grid.hs().ravel(), points.hs())


def test_ravel_xy_and_reporj():
    grid = GriddedSkeleton.add_datavar('hs')(lon=(10,20,30), lat=(50,60,70))
    data = np.array([[1,2,3],[4,5,6],[7,8,9]])
    grid.set_hs(data)
    grid.proj.set((33,'W'))
    points = grid.ravel(proj='lonlat')
    glon, glat = grid.lonlat()
    lon, lat = points.xy(native=True)
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)
    np.testing.assert_array_almost_equal(grid.hs().ravel(), points.hs())