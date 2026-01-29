from geo_skeletons.managers.proj_manager import ProjManager
from geo_skeletons.managers.metadata_manager import MetaDataManager

def test_set_epsg():
    pm = ProjManager(crs=4326, metadata_manager=MetaDataManager(None), lon=(None, None), lat=(None, None), x=(None, None), y=(None,None))
    assert pm.crs() == 4326
    

def test_set_epsg_from_str():
    pm = ProjManager(crs='EPSG:4326',metadata_manager=MetaDataManager(None), lon=(None, None), lat=(None, None), x=(None, None), y=(None,None))
    assert pm.crs() == 4326
    

def test_set_proj4():
    pm = ProjManager(crs="+proj=ob_tran +o_proj=latlon +o_lat_p=45 +o_lon_p=30 +lon_0=0", metadata_manager=MetaDataManager(None), lon=(None, None), lat=(None, None), x=(None, None), y=(None,None))
    assert pm.crs() == "+proj=ob_tran +o_proj=latlon +o_lat_p=45 +o_lon_p=30 +lon_0=0"