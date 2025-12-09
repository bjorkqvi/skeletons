from geo_skeletons.managers.proj_manager import ProjManager
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