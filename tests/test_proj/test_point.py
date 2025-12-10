from geo_skeletons import PointSkeleton
import numpy as np

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