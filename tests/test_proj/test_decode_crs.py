from geo_skeletons.managers.proj_manager import decode_crs
from pyproj import CRS

def test_none():
    assert decode_crs(None) == (None, None, None, None, None)

def test_epsg():
    assert decode_crs(4326) == (4326, None, None, None, None)

def test_epsg_str():
    assert decode_crs('EPSG:4326') == (4326, None, None,None, None)

def test_proj4():
    proj4 = "+proj=ob_tran +o_proj=latlon +o_lat_p=45 +o_lon_p=30 +lon_0=0"
    assert decode_crs(proj4) == (None, proj4, None, None, None)
    
def test_dict():
    cf_dict = {'key': 'value'}
    assert decode_crs(cf_dict) == (None, None, cf_dict, None, None)
    

def test_utm():
    utm = (33,'W')
    assert decode_crs(utm) == (None, None, None, utm, None)

def test_crs():
    crs = CRS.from_epsg(4326)
    assert decode_crs(crs) == (None, None, None, None, crs)