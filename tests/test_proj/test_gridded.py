from geo_skeletons import GriddedSkeleton
import numpy as np
import pytest
def test_lonlat():
    proj4 = '+proj=ob_tran +o_proj=longlat +lon_0=-40 +o_lat_p=22 +R=6.371e+06 +no_defs'
    rlon = np.array([5.53, 5.57, 5.61, 5.65, 5.69, 5.73, 5.77, 5.81, 5.85, 5.89])
    rlat = np.array([-14.35, -14.31, -14.27])
    lat = np.array([53.24779 , 53.241985, 53.236137, 53.230247, 53.22432 , 53.218346,53.212334, 53.206284, 53.20019 , 53.194057, 53.287342, 53.28153 ,53.275673, 53.26978 , 53.263844, 53.257866, 53.251846, 53.24579 ,53.23969 , 53.233547, 53.326893, 53.32107 , 53.315212, 53.30931 ,53.303368, 53.297382, 53.29136 , 53.285294, 53.279186, 53.273037])
    lon =np.array([-31.023573, -30.959545, -30.895535, -30.831545, -30.767574,
       -30.703623, -30.639692, -30.575779, -30.511889, -30.448015,
       -31.01358 , -30.949484, -30.885405, -30.821346, -30.757305,
       -30.693285, -30.629286, -30.565304, -30.501343, -30.437403,
       -31.00357 , -30.939404, -30.875256, -30.811127, -30.747019,
       -30.682928, -30.618858, -30.55481 , -30.49078 , -30.42677 ])
    points = GriddedSkeleton(x=rlon, y=rlat, crs=proj4)

    np.testing.assert_array_almost_equal(points.x(), rlon, decimal=5)
    np.testing.assert_array_almost_equal(points.y(), rlat, decimal=5)
    
    assert points.lon() is None
    assert points.lat() is None

    lo, la = points.lonlat()
    
    np.testing.assert_array_almost_equal(lo, lon, decimal=5)
    np.testing.assert_array_almost_equal(la, lat, decimal=5)

def test_xy_utm():
    
    lon = np.array([-31.023573, -30.959545, -30.895535, -30.831545, -30.767574, -30.703623, -30.639692, -30.575779, -30.511889, -30.448015])
    lat =np.array([53.24779 , 53.241985, 53.236137, 53.230247, 53.22432 , 53.218346,53.212334, 53.206284, 53.20019 , 53.194057])
    points = GriddedSkeleton(lon=lon, lat=lat)
    assert points.proj.crs() == (25, 'U')
    
    
    points = GriddedSkeleton(x=lon, y=lat)
    assert points.proj.crs() is None

def test_utm_request():
    lon = np.array([-31.023573, -30.959545, -30.895535, -30.831545, -30.767574, -30.703623, -30.639692, -30.575779, -30.511889, -30.448015])
    lat =np.array([53.24779 , 53.241985, 53.236137, 53.230247, 53.22432 , 53.218346,53.212334, 53.206284, 53.20019 , 53.194057])

    points = GriddedSkeleton(lon=lon, lat=lat)
    
    assert points.proj.crs() == (25, 'U')

    x, y = points.xy()

    points = GriddedSkeleton(lon=lon, lat=lat, crs=(26, 'U'))
    xx, yy = points.xy(crs=(25,'U'))

    np.testing.assert_array_almost_equal(x,xx)
    np.testing.assert_array_almost_equal(y,yy)


def test_utm_lonlat():
    lon = np.array([-31.023573, -30.959545, -30.895535, -30.831545, -30.767574, -30.703623, -30.639692, -30.575779, -30.511889, -30.448015])
    lat =np.array([53.24779 , 53.241985, 53.236137, 53.230247, 53.22432 , 53.218346,53.212334, 53.206284, 53.20019 , 53.194057])
    lat = list(lat)
    lat.sort()
    lat = np.array(lat)
    points = GriddedSkeleton(lon=lon, lat=lat)
    assert points.proj.crs() == (25, 'U')
    lo, la = points.lon(), points.lat()

    np.testing.assert_array_almost_equal(lon, lo)

    np.testing.assert_array_almost_equal(lat, la)

    lo, la = points.lon(), points.lat()
    np.testing.assert_array_almost_equal(lon, lo)
    np.testing.assert_array_almost_equal(lat, la)
