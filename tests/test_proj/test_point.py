from geo_skeletons import PointSkeleton
import numpy as np
import pytest
def test_xy():
    proj4 = '+proj=ob_tran +o_proj=longlat +lon_0=-40 +o_lat_p=22 +R=6.371e+06 +no_defs'
    lon = np.array([-31.023573, -30.959545, -30.895535, -30.831545, -30.767574, -30.703623, -30.639692, -30.575779, -30.511889, -30.448015])
    lat =np.array([53.24779 , 53.241985, 53.236137, 53.230247, 53.22432 , 53.218346,53.212334, 53.206284, 53.20019 , 53.194057])
    points = PointSkeleton(lon=lon, lat=lat, crs=proj4)
    rlon = np.array([5.53, 5.57, 5.61, 5.65, 5.69, 5.73, 5.77, 5.81, 5.85, 5.89])
    rlat = np.array([-14.35]*len(rlon))

    np.testing.assert_array_almost_equal(points.x(), rlon, decimal=5)
    np.testing.assert_array_almost_equal(points.y(), rlat, decimal=5)
    
def test_xy_utm():
    
    lon = np.array([-31.023573, -30.959545, -30.895535, -30.831545, -30.767574, -30.703623, -30.639692, -30.575779, -30.511889, -30.448015])
    lat =np.array([53.24779 , 53.241985, 53.236137, 53.230247, 53.22432 , 53.218346,53.212334, 53.206284, 53.20019 , 53.194057])
    points = PointSkeleton(lon=lon, lat=lat)
    
    assert points.proj.crs() == (25, 'U')
    
    
    points = PointSkeleton(x=lon, y=lat)
    assert points.proj.crs() is None

def test_utm_request():
    lon = np.array([-31.023573, -30.959545, -30.895535, -30.831545, -30.767574, -30.703623, -30.639692, -30.575779, -30.511889, -30.448015])
    lat =np.array([53.24779 , 53.241985, 53.236137, 53.230247, 53.22432 , 53.218346,53.212334, 53.206284, 53.20019 , 53.194057])
    points = PointSkeleton(lon=lon, lat=lat)
    
    assert points.proj.crs() == (25, 'U')

    x, y = points.xy()

    points = PointSkeleton(lon=lon, lat=lat, crs=(26, 'U'))
    xx, yy = points.xy(crs=(25,'U'))

    np.testing.assert_array_almost_equal(x,xx)
    np.testing.assert_array_almost_equal(y,yy)

def test_utm_request_cartesian():
    lon = np.array([-31.023573, -30.959545, -30.895535, -30.831545, -30.767574, -30.703623, -30.639692, -30.575779, -30.511889, -30.448015])
    lat =np.array([53.24779 , 53.241985, 53.236137, 53.230247, 53.22432 , 53.218346,53.212334, 53.206284, 53.20019 , 53.194057])
    points = PointSkeleton(lon=lon, lat=lat)
    
    assert points.proj.crs() == (25, 'U')

    x25, y25 = points.xy()
    points.proj.set((26,'U'))
    x26, y26 = points.xy()


    points = PointSkeleton(x=x25, y=y25, crs=(25, 'U'))
    x, y = points.xy(crs=(25,'U'))
    np.testing.assert_array_almost_equal(x25,x)
    np.testing.assert_array_almost_equal(y25,y)

    x, y = points.xy(crs=(26,'U'))
    np.testing.assert_array_almost_equal(x26,x, decimal=3)
    np.testing.assert_array_almost_equal(y26,y, decimal=3)

def test_utm_lonlat():
    lon = np.array([-31.023573, -30.959545, -30.895535, -30.831545, -30.767574, -30.703623, -30.639692, -30.575779, -30.511889, -30.448015])
    lat =np.array([53.24779 , 53.241985, 53.236137, 53.230247, 53.22432 , 53.218346,53.212334, 53.206284, 53.20019 , 53.194057])
    points = PointSkeleton(lon=lon, lat=lat)
    assert points.proj.crs() == (25, 'U')
    lo, la = points.lonlat()
    np.testing.assert_array_almost_equal(lon, lo)
    np.testing.assert_array_almost_equal(lat, la)

    lo, la = points.lonlat(crs=(26,'U'))
    np.testing.assert_array_almost_equal(lon, lo)
    np.testing.assert_array_almost_equal(lat, la)

def test_utm_lonlat_cartesian():
    lon = np.array([-31.023573, -30.959545, -30.895535, -30.831545, -30.767574, -30.703623, -30.639692, -30.575779, -30.511889, -30.448015])
    lat =np.array([53.24779 , 53.241985, 53.236137, 53.230247, 53.22432 , 53.218346,53.212334, 53.206284, 53.20019 , 53.194057])
    points = PointSkeleton(lon=lon, lat=lat)
    assert points.proj.crs() == (25, 'U')
    x25, y25 = points.xy()
    x26, y26 = points.xy(crs=(26,'U'))

    p25 = PointSkeleton(x=x25, y=y25, crs=(25,'U'))
    p26 = PointSkeleton(x=x26, y=y26, crs=(26,'U'))
    
    lo, la = p25.lonlat()
    np.testing.assert_array_almost_equal(lon, lo)
    np.testing.assert_array_almost_equal(lat, la)

    lo, la = p26.lonlat()
    np.testing.assert_array_almost_equal(lon, lo)
    np.testing.assert_array_almost_equal(lat, la)

    p26.proj.set((4,'E')) # Wrong utm zone
    lo, la = p26.lonlat()
    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(lon, lo)
    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(lat, la)

    lo, la = p26.lonlat(crs=(26,'U'))
    np.testing.assert_array_almost_equal(lon, lo)
    np.testing.assert_array_almost_equal(lat, la)