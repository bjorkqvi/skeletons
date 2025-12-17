from geo_skeletons import PointSkeleton, GriddedSkeleton
import pytest
import numpy as np
def test_cant_go_to_gridded():
    grid = GriddedSkeleton(x=10, y=20)
    points = PointSkeleton(x=10, y=20)
    grid2 = GriddedSkeleton(lon=5, lat=6)

    with pytest.raises(NotImplementedError):
        grid.resample.grid(grid2, engine='ravel')

    with pytest.raises(NotImplementedError):
        points.resample.grid(grid2, engine='ravel')

def test_ravel_lonlat():
    grid = GriddedSkeleton(lon=(10,20,30), lat=(50,60,70))
    points = grid.ravel()
    glon, glat = grid.lonlat()
    lon, lat = points.lonlat()
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)


def test_ravel_xy():
    grid = GriddedSkeleton(x=(10,20,30), y=(50,60,70))
    points = grid.ravel()
    glon, glat = grid.xy()
    lon, lat = points.xy()
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)


def test_ravel_lonlat_and_reproj():
    grid = GriddedSkeleton(lon=(10,20,30), lat=(50,60,70))
    points = grid.ravel(proj='xy')
    glon, glat = grid.xy()
    lon, lat = points.xy(native=True)
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)


def test_ravel_xy_and_reporj():
    grid = GriddedSkeleton(x=(10,20,30), y=(50,60,70))
    grid.proj.set((33,'W'))
    points = grid.ravel(proj='lonlat')
    glon, glat = grid.lonlat()
    lon, lat = points.xy(native=True)
    np.testing.assert_array_almost_equal(glon, lon)
    np.testing.assert_array_almost_equal(glat, lat)