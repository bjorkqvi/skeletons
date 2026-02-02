from geo_skeletons import PointSkeleton
import geo_parameters as gp
from geo_skeletons.classes import WindGrid

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
    points2.proj.set(4326)

    assert points2.ds().hs.grid_mapping == 'wgs84'
    assert points2.ds().lon.grid_mapping == 'wgs84'


def test_reproj_southerly_winds_rotated_grid_lon0_0_u_da():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(lat=10, lon=(-180, 180), crs=proj4)
    wind.set_spacing(nx=181)
    wind.set_ff(10)
    wind.set_dd(180)

    assert wind.dd(data_array=True, rotated=True).rotated_according_to =='crs'
    assert wind.u(data_array=True, rotated=True).rotated_according_to =='crs'
    assert wind.v(data_array=True, rotated=True).rotated_according_to =='crs'

    assert wind.dd(data_array=True).rotated_according_to =='wgs84'
    assert wind.u(data_array=True).rotated_according_to =='wgs84'
    assert wind.v(data_array=True).rotated_according_to =='wgs84'


def test_reproj_southerly_winds_rotated_grid_lon0_0_u_ds():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(lat=10, lon=(-180, 180), crs=proj4)
    wind.set_spacing(nx=181)
    wind.set_ff(10)
    wind.set_dd(180)

    ds_rot = wind.ds(compile=True, rotated=True)
    
    ds = wind.ds(compile=True)


    assert ds_rot.dd.rotated_according_to =='crs'
    assert ds_rot.u.rotated_according_to =='crs'
    assert ds_rot.v.rotated_according_to =='crs'

    assert ds.dd.rotated_according_to =='wgs84'
    assert ds.u.rotated_according_to =='wgs84'
    assert ds.v.rotated_according_to =='wgs84'

    assert ds.dd.grid_mapping =='wgs84'
    assert ds.u.grid_mapping =='wgs84'
    assert ds.v.grid_mapping =='wgs84'

    assert ds_rot.dd.grid_mapping =='wgs84'
    assert ds_rot.u.grid_mapping =='wgs84'
    assert ds_rot.v.grid_mapping =='wgs84'

def test_reproj_from_rotated_da():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(x=(-8194139, 8194139), y=(-8194139, 8194139), crs=proj4)
    wind.set_spacing(nx=10, ny=15)

    wind.set_ff(10)
    wind.set_dd(180)

    assert wind.dd(data_array=True, rotated=True).rotated_according_to =='crs'
    assert wind.u(data_array=True, rotated=True).rotated_according_to =='crs'
    assert wind.v(data_array=True, rotated=True).rotated_according_to =='crs'

    assert wind.dd(data_array=True).rotated_according_to =='wgs84'
    assert wind.u(data_array=True).rotated_according_to =='wgs84'
    assert wind.v(data_array=True).rotated_according_to =='wgs84'

def test_reproj_from_rotated_ds():
    proj4 = "+proj=laea +lat_0=90 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    wind = WindGrid(x=(-8194139, 8194139), y=(-8194139, 8194139), crs=proj4)
    wind.set_spacing(nx=10, ny=15)

    wind.set_ff(10)
    wind.set_dd(180)
    ds_rot = wind.ds(compile=True, rotated=True)
    
    ds = wind.ds(compile=True)


    assert ds_rot.dd.rotated_according_to =='crs'
    assert ds_rot.u.rotated_according_to =='crs'
    assert ds_rot.v.rotated_according_to =='crs'

    assert ds.dd.rotated_according_to =='wgs84'
    assert ds.u.rotated_according_to =='wgs84'
    assert ds.v.rotated_according_to =='wgs84'

    assert ds.dd.grid_mapping =='crs'
    assert ds.u.grid_mapping =='crs'
    assert ds.v.grid_mapping =='crs'

    assert ds_rot.dd.grid_mapping =='crs'
    assert ds_rot.u.grid_mapping =='crs'
    assert ds_rot.v.grid_mapping =='crs'
