from geo_skeletons import PointSkeleton, GriddedSkeleton
UTM = (34, 'V')
RLON =(26.21,27.37)
RLAT = (4.33,5.29)
LON = (18.95151691, 21.76951594)
LAT = (59.06608818, 58.69593138)
PROJ4 = '+proj=ob_tran +o_proj=longlat +lon_0=-40 +o_lat_p=22 +R=6.371e+06 +no_defs'

def test_point_xy_no_proj():
    data = PointSkeleton(x=(1,0), y=(5,6))
    assert data.proj.my_utm() is None

def test_point_xy_utm_proj():
    data = PointSkeleton(x=(1,0), y=(5,6), crs=UTM)
    assert data.proj.my_utm() == UTM

def test_point_xy_rot_proj():
    data = PointSkeleton(x=RLON, y=RLAT, crs=PROJ4)
    assert data.proj.my_utm() == UTM

def test_point_lonlat():
    data = PointSkeleton(lon=LON, lat=LAT)
    assert data.proj.my_utm() == UTM

def test_point_lonlat_modified_utm():
    data = PointSkeleton(lon=LON, lat=LAT, crs=(33,'W'))
    assert data.proj.my_utm() == (33,'W')
    assert data.proj.my_utm(optimal=True) == UTM

def test_point_lonlat_rot_proj():
    data = PointSkeleton(lon=LON, lat=LAT, crs=PROJ4)
    assert data.proj.my_utm() == UTM


def test_gridded_xy_no_proj():
    data = GriddedSkeleton(x=(1,0), y=(5,6))
    data.set_spacing(nx=10, ny=20)
    assert data.proj.my_utm() is None

def test_gridded_xy_utm_proj():
    data =GriddedSkeleton(x=(1,0), y=(5,6), crs=UTM)
    data.set_spacing(nx=10, ny=20)
    assert data.proj.my_utm() == UTM

def test_gridded_xy_rot_proj():
    data = GriddedSkeleton(x=RLON, y=RLAT, crs=PROJ4)
    data.set_spacing(nx=10, ny=20)
    assert data.proj.my_utm() == UTM

def test_gridded_lonlat():
    data = GriddedSkeleton(lon=LON, lat=LAT)
    data.set_spacing(nx=10, ny=20)
    assert data.proj.my_utm() == UTM

def test_gridded_lonlat_modified_utm():
    data = GriddedSkeleton(lon=LON, lat=LAT, crs=(33,'W'))
    data.set_spacing(nx=10, ny=20)
    assert data.proj.my_utm() == (33,'W')
    assert data.proj.my_utm(optimal=True) == UTM

def test_gridded_lonlat_rot_proj():
    data = GriddedSkeleton(lon=LON, lat=LAT, crs=PROJ4)
    data.set_spacing(nx=10, ny=20)
    assert data.proj.my_utm() == UTM
