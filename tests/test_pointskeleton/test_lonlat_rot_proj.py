from geo_skeletons import PointSkeleton

import numpy as np
from geo_skeletons.distance_funcs import dx_to_dlon, get_dist_point

LON = (20,21,22)
LAT = (58,59,60)
X = (440892.10517494, 500000.        , 555776.26675182)
Y = (6429147.61178793, 6540052.01822338, 6651832.73615611)
RLON = (27.39474293, 26.89012954, 26.3601537)
RLAT = (4.12990936, 5.13984603, 6.12947612)
CRS = (34, 'V')
PROJ4 = '+proj=ob_tran +o_proj=longlat +lon_0=-40 +o_lat_p=22 +R=6.371e+06 +no_defs'
RES = get_dist_point(X,Y)
ROTRES = get_dist_point(RLON, RLAT)

def test_resolution():
    data = PointSkeleton(lon=LON, lat=LAT, crs=PROJ4)
    np.testing.assert_almost_equal(data.resolution(), RES)

def test_dx_dy():
    data = PointSkeleton(lon=LON, lat=LAT, crs=PROJ4)
    np.testing.assert_almost_equal(data.dx(), np.median(ROTRES))
    np.testing.assert_almost_equal(data.dy(), np.median(ROTRES))
    assert data.dx(strict=True) is None
    dlon = dx_to_dlon(dx=data.dmx(), lon=data.lon()[1], lat=data.lat()[1])
    np.testing.assert_almost_equal(data.dx(native=True), dlon)
    assert data.dy(strict=True) is None
    np.testing.assert_almost_equal(data.dy(native=True), data.dmy()/111_000, decimal=2)

def test_dmx_dmy():
    data = PointSkeleton(lon=LON, lat=LAT, crs=PROJ4)
    np.testing.assert_almost_equal(data.dmx(), np.median(RES))
    np.testing.assert_almost_equal(data.dmy(), np.median(RES))
    assert data.dmx(strict=True) is None
    dlon = dx_to_dlon(dx=data.dmx(), lon=data.lon()[1], lat=data.lat()[1])
    np.testing.assert_almost_equal(data.dmx(native=True), dlon)
    assert data.dmy(strict=True) is None
    np.testing.assert_almost_equal(data.dmy(native=True), data.dmy()/111_000, decimal=2)

def test_dlon_dlat():
    data = PointSkeleton(lon=LON, lat=LAT, crs=PROJ4)
    np.testing.assert_almost_equal(data.dlat(), data.dmy()/111_000, decimal=2)
    dlon = dx_to_dlon(dx=data.dmx(), lon=data.lon()[1], lat=data.lat()[1])
    np.testing.assert_almost_equal(data.dlon(), dlon)
    np.testing.assert_almost_equal(data.dlon(strict=True), dlon)
    np.testing.assert_almost_equal(data.dlon(native=True), dlon)
    np.testing.assert_almost_equal(data.dlat(strict=True), data.dmy()/111_000, decimal=2)
    np.testing.assert_almost_equal(data.dlat(native=True), data.dmy()/111_000, decimal=2)