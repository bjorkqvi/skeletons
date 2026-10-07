from geo_skeletons import PointSkeleton
import geo_parameters as gp
from geo_skeletons.classes import WindGrid

def test_no_meta():
    points = PointSkeleton(x=0, y=0)
    assert points.meta.get('crs') == {'grid_mapping_name': 'local_cartesian', 'crs_wkt': 'ENGCRS["Local Cartesian",EDATUM["Local datum"],CS[Cartesian,2],AXIS["x",east,ORDER[1],LENGTHUNIT["metre",1]],AXIS["y",north,ORDER[2],LENGTHUNIT["metre",1]]]', 'spatial_ref': 'ENGCRS["Local Cartesian",EDATUM["Local datum"],CS[Cartesian,2],AXIS["x",east,ORDER[1],LENGTHUNIT["metre",1]],AXIS["y",north,ORDER[2],LENGTHUNIT["metre",1]]]', 'units': 'metre', 'description': 'Local engineering Cartesian CRS (no geodetic tie).'}

def test_utm():
    points = PointSkeleton(x=0, y=0, crs=(32, 'W'))
    assert points.meta.get('crs') == {'grid_mapping_name': 'transverse_mercator', 'utm_zone_number': 32, 'utm_latitude_band': 'W', 'utm_hemisphere': 'N', 'description': 'UTM zone 32W (latitude band W); Hemisphere: N.'}

def test_proj4():
    points = PointSkeleton(x=0, y=0, crs=(32, 'W'))
    assert points.meta.get('crs') =={'grid_mapping_name': 'transverse_mercator', 'utm_zone_number': 32, 'utm_latitude_band': 'W', 'utm_hemisphere': 'N', 'description': 'UTM zone 32W (latitude band W); Hemisphere: N.'}
    points.proj.set("+proj=ob_tran +o_proj=latlon +o_lat_p=45 +o_lon_p=30 +lon_0=0")
    assert points.meta.get('crs') == {'semi_major_axis': 6378137.0, 'inverse_flattening': 298.257223563, 'grid_mapping_name': 'rotated_latitude_longitude', 'grid_north_pole_latitude': 45.0, 'grid_north_pole_longitude': -180.0, 'crs_wkt': 'GEOGCRS["unnamed",BASEGEOGCRS["unknown",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]],ID["EPSG",6326]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8901]]],DERIVINGCONVERSION["unknown",METHOD["PROJ ob_tran o_proj=latlon"],PARAMETER["o_lat_p",45,ANGLEUNIT["degree",0.0174532925199433,ID["EPSG",9122]]],PARAMETER["o_lon_p",30,ANGLEUNIT["degree",0.0174532925199433,ID["EPSG",9122]]],PARAMETER["lon_0",0,ANGLEUNIT["degree",0.0174532925199433,ID["EPSG",9122]]]],CS[ellipsoidal,2],AXIS["longitude",east,ORDER[1],ANGLEUNIT["degree",0.0174532925199433,ID["EPSG",9122]]],AXIS["latitude",north,ORDER[2],ANGLEUNIT["degree",0.0174532925199433,ID["EPSG",9122]]]]', 'spatial_ref': 'GEOGCRS["unnamed",BASEGEOGCRS["unknown",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]],ID["EPSG",6326]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8901]]],DERIVINGCONVERSION["unknown",METHOD["PROJ ob_tran o_proj=latlon"],PARAMETER["o_lat_p",45,ANGLEUNIT["degree",0.0174532925199433,ID["EPSG",9122]]],PARAMETER["o_lon_p",30,ANGLEUNIT["degree",0.0174532925199433,ID["EPSG",9122]]],PARAMETER["lon_0",0,ANGLEUNIT["degree",0.0174532925199433,ID["EPSG",9122]]]],CS[ellipsoidal,2],AXIS["longitude",east,ORDER[1],ANGLEUNIT["degree",0.0174532925199433,ID["EPSG",9122]]],AXIS["latitude",north,ORDER[2],ANGLEUNIT["degree",0.0174532925199433,ID["EPSG",9122]]]]'}

def test_dict():
    crs_metadata = {
    "grid_mapping_name": "lambert_conformal_conic",
    "standard_parallel": [63.3, 63.3],
    "longitude_of_central_meridian": 15.0,
    "latitude_of_projection_origin": 63.3,
    "earth_radius": 6371000.0,
    }
    points = PointSkeleton(x=0, y=0, crs=crs_metadata)
    assert points.meta.get('crs') == {'semi_major_axis': 6371000.0, 'inverse_flattening': 0.0, 'grid_mapping_name': 'lambert_conformal_conic', 'standard_parallel': (63.3, 63.3), 'latitude_of_projection_origin': 63.3, 'longitude_of_central_meridian': 15.0, 'false_easting': 0.0, 'false_northing': 0.0, 'crs_wkt': 'PROJCRS["undefined",BASEGEOGCRS["undefined",DATUM["undefined",ELLIPSOID["undefined",6371000,0,LENGTHUNIT["metre",1,ID["EPSG",9001]]]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8901]]],CONVERSION["unknown",METHOD["Lambert Conic Conformal (2SP)",ID["EPSG",9802]],PARAMETER["Latitude of 1st standard parallel",63.3,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8823]],PARAMETER["Latitude of 2nd standard parallel",63.3,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8824]],PARAMETER["Latitude of false origin",63.3,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8821]],PARAMETER["Longitude of false origin",15,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8822]],PARAMETER["Easting at false origin",0,LENGTHUNIT["metre",1],ID["EPSG",8826]],PARAMETER["Northing at false origin",0,LENGTHUNIT["metre",1],ID["EPSG",8827]]],CS[Cartesian,2],AXIS["(E)",east,ORDER[1],LENGTHUNIT["metre",1,ID["EPSG",9001]]],AXIS["(N)",north,ORDER[2],LENGTHUNIT["metre",1,ID["EPSG",9001]]]]', 'spatial_ref': 'PROJCRS["undefined",BASEGEOGCRS["undefined",DATUM["undefined",ELLIPSOID["undefined",6371000,0,LENGTHUNIT["metre",1,ID["EPSG",9001]]]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8901]]],CONVERSION["unknown",METHOD["Lambert Conic Conformal (2SP)",ID["EPSG",9802]],PARAMETER["Latitude of 1st standard parallel",63.3,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8823]],PARAMETER["Latitude of 2nd standard parallel",63.3,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8824]],PARAMETER["Latitude of false origin",63.3,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8821]],PARAMETER["Longitude of false origin",15,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8822]],PARAMETER["Easting at false origin",0,LENGTHUNIT["metre",1],ID["EPSG",8826]],PARAMETER["Northing at false origin",0,LENGTHUNIT["metre",1],ID["EPSG",8827]]],CS[Cartesian,2],AXIS["(E)",east,ORDER[1],LENGTHUNIT["metre",1,ID["EPSG",9001]]],AXIS["(N)",north,ORDER[2],LENGTHUNIT["metre",1,ID["EPSG",9001]]]]'}


def test_epsg():
    points = PointSkeleton(x=0, y=0, crs=4326)

    assert points.meta.get('crs').get('epsg_code') == 'EPSG:4326'

def test_epsg_spherical():
    points = PointSkeleton(lat=0, lon=0)
    assert points.meta.get('crs').get('epsg_code') == 'EPSG:4326'

def test_crs():
    points = PointSkeleton(x=0, y=0)
    points.proj.set(4226)
    assert points.meta.get('crs').get('epsg_code') == 'EPSG:4226'
    crs = points.proj.crs() # Pyproj CRS object
    points.proj.set(crs)

    assert 'crs_wkt' in points.meta.get('crs').keys()

def test_crs_with_ds_compile():
    points = PointSkeleton(x=0, y=0)
    points.proj.set(4226)
    assert points.meta.get('crs').get('epsg_code') == 'EPSG:4226'
    
    ds = points.ds()
    assert 'crs_wkt' in ds.crs.attrs
    ds = points.ds(compile=True)
    assert 'crs_wkt' in ds.crs.attrs

def test_on_class():
    cls = PointSkeleton.add_datavar(gp.wave.Hs)
    points = cls(x=0, y=0)
    points.set_hs(3)
    points.proj.set(4226)
    assert points.ds().hs.grid_mapping == 'crs'
    assert points.ds().x.grid_mapping == 'crs'

    points2 = cls(lon=0, lat=0)
    points2.set_hs(3)


    assert points2.ds().hs.grid_mapping == 'crs'
    assert points2.ds().lon.grid_mapping == 'crs'


