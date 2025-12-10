from geo_skeletons import PointSkeleton
import geo_parameters as gp
def test_no_meta():
    points = PointSkeleton(x=0, y=0)
    assert points.meta.get('crs') == {}

def test_utm():
    points = PointSkeleton(x=0, y=0, crs=(32, 'W'))
    assert points.meta.get('crs') == {'utm_zone': 32, 'utm_letter': 'W'}

def test_proj4():
    points = PointSkeleton(x=0, y=0, crs=(32, 'W'))
    assert points.meta.get('crs') == {'utm_zone': 32, 'utm_letter': 'W'}
    points.proj.set("+proj=ob_tran +o_proj=latlon +o_lat_p=45 +o_lon_p=30 +lon_0=0")
    assert points.meta.get('crs') == {'proj4':"+proj=ob_tran +o_proj=latlon +o_lat_p=45 +o_lon_p=30 +lon_0=0"}

def test_dict():
    crs_metadata = {
    "grid_mapping_name": "lambert_conformal_conic",
    "standard_parallel": [63.3, 63.3],
    "longitude_of_central_meridian": 15.0,
    "latitude_of_projection_origin": 63.3,
    "earth_radius": 6371000.0,
    }
    points = PointSkeleton(x=0, y=0, crs=(32, 'W'))
    assert points.meta.get('crs') == {'utm_zone': 32, 'utm_letter': 'W'}
    points.proj.set(crs_metadata)
    assert points.meta.get('crs') == crs_metadata


def test_epsg():
    points = PointSkeleton(x=0, y=0, crs=(32, 'W'))
    assert points.meta.get('crs') == {'utm_zone': 32, 'utm_letter': 'W'}
    points.proj.set(4326)
    assert points.meta.get('crs') == {'epsg': 4326}

def test_epsg_spherical():
    points = PointSkeleton(lat=0, lon=0, crs=(32, 'W'))
    assert points.meta.get('crs') == {'utm_zone': 32, 'utm_letter': 'W'}
    assert points.meta.get('wgs84') == {'epsg': 4326}

def test_crs():
    points = PointSkeleton(x=0, y=0)
    points.proj.set(4326)
    assert points.meta.get('crs') == {'epsg': 4326}
    crs = points.proj.crs() # Pyproj CRS object
    points.proj.set(crs)

    assert 'crs_wkt' in points.meta.get('crs').keys()

def test_crs_with_ds_compile():
    points = PointSkeleton(x=0, y=0)
    points.proj.set(4326)
    assert points.meta.get('crs') == {'epsg': 4326}
    
    ds = points.ds()
    assert 'crs_wkt' not in ds.crs.attrs
    ds = points.ds(compile=True)
    assert 'crs_wkt' in ds.crs.attrs

def test_on_class():
    cls = PointSkeleton.add_datavar(gp.wave.Hs)
    points = cls(x=0, y=0)
    points.set_hs(3)
    points.proj.set(4326)
    assert points.ds().hs.grid_mapping == 'crs'
    assert points.ds().x.grid_mapping == 'crs'

    points2 = cls(lon=0, lat=0)
    points2.set_hs(3)
    points2.proj.set(4325)

    assert points2.ds().hs.grid_mapping == 'wgs84'
    assert points2.ds().lon.grid_mapping == 'wgs84'
    breakpoint()