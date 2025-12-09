from geo_skeletons.managers.proj_manager import decode_crs
from pyproj import CRS

def test_none():
    assert decode_crs(None) == (None, None)

def test_epsg():
    assert decode_crs(4326) == (4326, None)

def test_epsg_str():
    assert decode_crs('EPSG:4326') == (4326, None)

def test_proj4():
    proj4 = "+proj=ob_tran +o_proj=latlon +o_lat_p=45 +o_lon_p=30 +lon_0=0"
    assert decode_crs(proj4) == (None, proj4)
    