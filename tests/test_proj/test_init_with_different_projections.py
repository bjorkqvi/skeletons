from geo_skeletons import GriddedSkeleton, PointSkeleton
from pyproj import CRS
import pytest
from geo_skeletons.errors import ProjectionError
PROJ4 = '+proj=ob_tran +o_proj=longlat +lon_0=-40 +o_lat_p=22 +R=6.371e+06 +no_defs'
crs_metadata = {
    "grid_mapping_name": "lambert_conformal_conic",
    "standard_parallel": [63.3, 63.3],
    "longitude_of_central_meridian": 15.0,
    "latitude_of_projection_origin": 63.3,
    "earth_radius": 6371000.0,
    }

UTM = (32, 'U')
def test_lonlat_has_wgs84():
    data = GriddedSkeleton(lon=(10, 20), lat=(50, 60))
    assert data.proj.utm() == UTM
    assert data.proj.optimal_utm() == UTM
    assert data.proj.crs() == CRS.from_epsg(4326)

def test_noproj_xy_is_none():
    data = GriddedSkeleton(x=(10, 20), y=(50, 60))
    assert data.proj.utm() is None
    assert data.proj.optimal_utm() is None
    assert data.proj.is_engineering()


def test_xy_utm_proj():
    data = GriddedSkeleton(x=(571666.44750366, 1111654.03336625), y=(5539109.81566836, 6702495.15549543))
    data.proj.set(UTM)
    assert data.proj.utm() == UTM
    assert data.proj.crs() == UTM
    assert data.proj.optimal_utm() == UTM

def test_xy_rot():
    data = GriddedSkeleton(x=(29.64961796, 25.77779487), y=(-5.51605865, 5.31451654))
    data.proj.set(PROJ4)

    assert data.proj.crs() == CRS.from_proj4(PROJ4)
    assert data.proj.utm() == (31,'U')
    assert data.proj.optimal_utm() == (31,'U')
    data.proj.set_utm(UTM)
    assert data.proj.crs() == CRS.from_proj4(PROJ4)
    assert data.proj.utm() == UTM
    assert data.proj.optimal_utm() == (31,'U')


def test_xy_setting_crs_sets_utm():
    data = GriddedSkeleton(x=(571666.44750366, 1111654.03336625), y=(5539109.81566836, 6702495.15549543))
    data.proj.set(UTM)
    assert data.proj.utm() == UTM
    assert data.proj.crs() == UTM
    assert data.proj.optimal_utm() == UTM
    
    data.proj.set(PROJ4)
    assert data.proj.crs() == CRS.from_proj4(PROJ4)
    
    assert data.proj.utm() == UTM
    data.proj.set((31,'U')) # This sets the CRS and should also set the utm so we don't have a conflict
    assert data.proj.utm() == (31,'U')
    assert data.proj.crs() == (31,'U')
    assert data.proj.optimal_utm() == (31,'U')

def test_xy_lambert():
    data = GriddedSkeleton(x=(-365687.98714664, 278150.80047618), y=(-1476844.93184955, -356292.85011904))
    data.proj.set(crs_metadata)
    assert data.proj.crs() == CRS.from_cf(crs_metadata)
    assert data.proj.utm() == (32,'U')
    assert data.proj.optimal_utm() == (32,'U')
    data.proj.set_utm(UTM)
    assert data.proj.crs() == CRS.from_cf(crs_metadata)
    assert data.proj.utm() == UTM
    assert data.proj.optimal_utm() == (32,'U')



def test_reset_utm():
    data = GriddedSkeleton(lon=(10, 20), lat=(50, 60))
    data.proj.set_utm(UTM)
    assert data.proj.utm() == UTM
    assert data.proj.crs() == CRS.from_epsg(4326)
    assert data.proj.optimal_utm() == UTM

    data.proj.set_utm((33,'W'))
    assert data.proj.utm() == (33,'W')
    assert data.proj.crs() == CRS.from_epsg(4326)

    data.proj.reset_utm()
    assert data.proj.utm() == UTM
    assert data.proj.crs() == CRS.from_epsg(4326)
    assert data.proj.optimal_utm() == UTM

def test_set_utm_sets_crs_if_crs_is_utm():
    data = GriddedSkeleton(x=(571666.44750366, 1111654.03336625), y=(5539109.81566836, 6702495.15549543))
    data.proj.set(UTM)
    assert data.proj.utm() == UTM
    assert data.proj.crs() == UTM
    assert data.proj.optimal_utm() == UTM

    data.proj.set_utm((31,'U'))
    assert data.proj.utm() == (31,'U')
    assert data.proj.crs() == (31,'U')
    assert data.proj.optimal_utm() == (31,'U')

def test_wgs84_is_protected():
    data = GriddedSkeleton(lon=(10, 20), lat=(50, 60))
    with pytest.raises(ProjectionError):
        data.proj.set((33,'W'))
