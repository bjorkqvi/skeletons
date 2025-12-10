from geo_skeletons.managers.proj_manager import ProjManager
from pyproj import CRS

def test_none():
    pm = ProjManager(metadata_manager=None, lon=(None, None), lat=(None, None))
    assert pm.crs() is None

def test_epsg():
    pm = ProjManager(metadata_manager=None, lon=(None, None), lat=(None, None))
    assert pm.crs() is None
    pm.set(4326)
    assert isinstance(pm.crs(), CRS)

def test_proj4():
    pm = ProjManager(metadata_manager=None, lon=(None, None), lat=(None, None))
    assert pm.crs() is None
    pm.set("+proj=ob_tran +o_proj=latlon +o_lat_p=45 +o_lon_p=30 +lon_0=0")
    assert isinstance(pm.crs(), CRS)