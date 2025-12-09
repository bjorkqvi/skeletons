from geo_skeletons.managers.proj_manager import ProjManager


def test_set_epsg():
    pm = ProjManager(crs=4326, metadata_manager=None)
    assert pm._epsg == 4326
    assert pm._proj4 is None

def test_set_epsg_from_str():
    pm = ProjManager(crs='EPSG:4326', metadata_manager=None)
    assert pm._epsg == 4326
    assert pm._proj4 is None

def test_set_proj4():
    pm = ProjManager(crs="+proj=ob_tran +o_proj=latlon +o_lat_p=45 +o_lon_p=30 +lon_0=0", metadata_manager=None)
    assert pm._epsg is None
    assert pm._proj4 == "+proj=ob_tran +o_proj=latlon +o_lat_p=45 +o_lon_p=30 +lon_0=0"