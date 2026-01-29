from geo_skeletons import PointSkeleton

import numpy as np
from geo_skeletons.distance_funcs import dx_to_dlon, get_dist_point

X = (382547.31054393, 494387.9110868 , 544599.00357443)
Y = (6549212.28592749, 6521088.97933779, 6506450.50819621)
CRS = (34, 'V')

RLON =(26.21,27.0,27.37)
RLAT = (4.33,5.0,5.29)
PROJ4 = '+proj=ob_tran +o_proj=longlat +lon_0=-40 +o_lat_p=22 +R=6.371e+06 +no_defs'
RES = get_dist_point(X, Y)
ROTRES = get_dist_point(RLON, RLAT)
def test_resolution():
    data = PointSkeleton(x=RLON, y=RLAT, crs=PROJ4)
    cart_data = PointSkeleton(x=X, y=Y, crs=CRS)
    
    np.testing.assert_almost_equal(data.resolution(), cart_data.resolution())
    np.testing.assert_almost_equal(data.resolution(), RES)

def test_dx_dy():
    data = PointSkeleton(x=RLON, y=RLAT, crs=PROJ4)
    np.testing.assert_almost_equal(data.dx(), np.median(ROTRES))
    np.testing.assert_almost_equal(data.dy(), np.median(ROTRES))
    np.testing.assert_almost_equal(data.dx(strict=True), np.median(ROTRES))
    np.testing.assert_almost_equal(data.dx(native=True), np.median(ROTRES))
    np.testing.assert_almost_equal(data.dy(strict=True), np.median(ROTRES))
    np.testing.assert_almost_equal(data.dy(native=True), np.median(ROTRES))

def test_dmx_dmy():
    data = PointSkeleton(x=RLON, y=RLAT, crs=PROJ4)
    np.testing.assert_almost_equal(data.dmx(), np.median(RES))
    np.testing.assert_almost_equal(data.dmy(), np.median(RES))
    assert data.dmx(strict=True) is None
    np.testing.assert_almost_equal(data.dmx(native=True), np.median(ROTRES))
    assert data.dmy(strict=True) is None
    np.testing.assert_almost_equal(data.dmy(native=True), np.median(ROTRES))

def test_dlon_dlat():
    data = PointSkeleton(x=RLON, y=RLAT, crs=PROJ4)
    np.testing.assert_almost_equal(data.dlat(), data.dmy()/111_000, decimal=2)
    dlon = dx_to_dlon(dx=data.dmx(), lon=data.lon()[1], lat=data.lat()[1])
    np.testing.assert_almost_equal(data.dlon(), dlon)
    assert data.dlon(strict=True) is None
    np.testing.assert_almost_equal(data.dlon(native=True), np.median(ROTRES))
    assert data.dlat(strict=True) is None
    np.testing.assert_almost_equal(data.dlat(native=True), np.median(ROTRES))