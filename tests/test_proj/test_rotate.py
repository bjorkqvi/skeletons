from geo_skeletons.managers.proj_manager import ProjManager
from geo_skeletons import PointSkeleton
import numpy as np



def test_ww3_4km():
    proj4 = '+proj=ob_tran +o_proj=longlat +lon_0=-40 +o_lat_p=22 +R=6.371e+06 +no_defs'
    pm = ProjManager(metadata_manager=None, crs=proj4)
    rlon = np.array([5.53, 5.57, 5.61, 5.65, 5.69, 5.73, 5.77, 5.81, 5.85, 5.89])
    rlat = np.array([-14.35]*len(rlon))
    lon = np.array([-31.023573, -30.959545, -30.895535, -30.831545, -30.767574, -30.703623, -30.639692, -30.575779, -30.511889, -30.448015])
    lat =np.array([53.24779 , 53.241985, 53.236137, 53.230247, 53.22432 , 53.218346,53.212334, 53.206284, 53.20019 , 53.194057])
    tlon, tlat = pm._lonlat(x=rlon, y=rlat)
    np.testing.assert_array_almost_equal(lon,tlon,decimal=5)
    np.testing.assert_array_almost_equal(lat,tlat, decimal=5)

    # Transform back
    relon, relat = pm._xy(lon=tlon, lat=tlat)
    np.testing.assert_array_almost_equal(relon,rlon,decimal=5)

    np.testing.assert_array_almost_equal(relat,rlat, decimal=5)


def test_meps():
    crs_metadata = {
    "grid_mapping_name": "lambert_conformal_conic",
    "standard_parallel": [63.3, 63.3],
    "longitude_of_central_meridian": 15.0,
    "latitude_of_projection_origin": 63.3,
    "earth_radius": 6371000.0,
    }
    pm = ProjManager(metadata_manager=None, crs=crs_metadata)

    x = np.array([-1060084.  , -1057584.  , -1055084.  , -1052584.  , -1050084. , -1047584.06, -1045084.06, -1042584.06, -1040084.06, -1037584.06])
    y = np.array([-1332517.9]*len(x))
    lon = np.array([0.27828066, 0.31179626, 0.34532003, 0.37885195, 0.41239201,0.44593935, 0.47949564, 0.51306002, 0.54663247, 0.58021299])
    lat = np.array([50.31961636, 50.32461057, 50.32959368, 50.33456568, 50.33952657,50.34447622, 50.34941488, 50.35434242, 50.35925882, 50.36416409])

    plon, plat = pm._lonlat(x=x, y=y)
    np.testing.assert_array_almost_equal(lon,plon,decimal=6)
    np.testing.assert_array_almost_equal(lat,plat, decimal=6)

    # Transform back
    relon, relat = pm._xy(lon=plon, lat=plat)
    np.testing.assert_array_almost_equal(relon,x,decimal=6)

    np.testing.assert_array_almost_equal(relat,y, decimal=6)


def test_utm():
    lon = np.array([6.1, 7.5, 10.9])

    lat = np.array([56.5, 58.9,63.9])
    points = PointSkeleton(lon=lon, lat=lat)
    assert points.utm.zone() == (32,'V')
    pm = ProjManager(metadata_manager=None, crs=32632)
    x, y = pm._xy(lon=lon, lat=lat)
    np.testing.assert_array_almost_equal(x, points.x(), decimal=2)
    np.testing.assert_array_almost_equal(y, points.y(), decimal=2)