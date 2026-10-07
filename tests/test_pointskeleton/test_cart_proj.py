import pytest

from geo_skeletons import PointSkeleton

import numpy as np
from geo_skeletons.distance_funcs import dx_to_dlon, get_dist_point

LON = (20,21,22)
LAT = (58,59,60)
X = (440892.10517494, 500000.        , 555776.26675182)
Y = (6429147.61178793, 6540052.01822338, 6651832.73615611)
CRS = (34, 'V')
RES = get_dist_point(X,Y)
LX = (-365687.98714664, -43768.59333523002,278150.80047618)
LY = (-1476844.93184955, -916568.8909842949, -356292.85011904)
crs_metadata = {
    "grid_mapping_name": "lambert_conformal_conic",
    "standard_parallel": [63.3, 63.3],
    "longitude_of_central_meridian": 15.0,
    "latitude_of_projection_origin": 63.3,
    "earth_radius": 6371000.0,
    }
LRES = get_dist_point(LX,LY)

def test_resolution_lambert():
    data = PointSkeleton(x=LX, y=LY, crs=crs_metadata)
    np.testing.assert_almost_equal(data.resolution(), LRES)
    x, y = data.xy(crs=data.proj.utm())
    # Cartesian projection uses own projection. not UTM
    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(LRES, get_dist_point(tuple(x),tuple(y)))

def test_dx_dy_lambert():
    data = PointSkeleton(x=LX, y=LY, crs=crs_metadata)
    np.testing.assert_almost_equal(data.dx(), np.median(LRES))
    np.testing.assert_almost_equal(data.dy(), np.median(LRES))
    np.testing.assert_almost_equal(data.dx(strict=True), np.median(LRES))
    np.testing.assert_almost_equal(data.dy(strict=True), np.median(LRES))
    np.testing.assert_almost_equal(data.dx(native=True), np.median(LRES))
    np.testing.assert_almost_equal(data.dy(native=True), np.median(LRES), decimal=2)
def test_dmx_dmy_lambert():
    data = PointSkeleton(x=LX, y=LY, crs=crs_metadata)
    np.testing.assert_almost_equal(data.dmx(), np.median(LRES))
    np.testing.assert_almost_equal(data.dmy(), np.median(LRES))
    np.testing.assert_almost_equal(data.dmx(strict=True), np.median(LRES))
    np.testing.assert_almost_equal(data.dmy(strict=True), np.median(LRES))
    np.testing.assert_almost_equal(data.dmx(native=True), np.median(LRES))
    np.testing.assert_almost_equal(data.dmy(native=True), np.median(LRES))

def test_dlon_dlat_lambert():
    data = PointSkeleton(x=LX, y=LY, crs=crs_metadata)
    np.testing.assert_almost_equal(data.dlat(), data.dmy()/111_000, decimal=1)
    dlon = dx_to_dlon(dx=data.dmx(), lon=data.lon()[1], lat=data.lat()[1])
    np.testing.assert_almost_equal(data.dlon(), dlon)
    assert data.dlon(strict=True) is None
    assert data.dlat(strict=True) is None
    np.testing.assert_almost_equal(data.dlon(native=True), data.dmx())
    np.testing.assert_almost_equal(data.dlat(native=True), data.dmy())

def test_resolution_cart():
    data = PointSkeleton(x=X, y=Y, crs=CRS)
    np.testing.assert_almost_equal(data.resolution(), RES)

def test_resolution_cart_engineering():
    data = PointSkeleton(x=X, y=Y)
    np.testing.assert_almost_equal(data.resolution(), RES)


def test_dx_dy_cart():
    data = PointSkeleton(x=X, y=Y, crs=CRS)
    np.testing.assert_almost_equal(data.dx(), np.median(RES))
    np.testing.assert_almost_equal(data.dy(), np.median(RES))
    np.testing.assert_almost_equal(data.dx(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dy(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dx(native=True), np.median(RES))
    np.testing.assert_almost_equal(data.dy(native=True), data.dmy(), decimal=2)
    
def test_dx_dy_cart_engineering():
    data = PointSkeleton(x=X, y=Y)
    np.testing.assert_almost_equal(data.dx(), np.median(RES))
    np.testing.assert_almost_equal(data.dy(), np.median(RES))
    np.testing.assert_almost_equal(data.dx(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dy(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dx(native=True), np.median(RES))
    np.testing.assert_almost_equal(data.dy(native=True), data.dmy(), decimal=2)

def test_dmx_dmy_cart():
    data = PointSkeleton(x=X, y=Y, crs=CRS)
    np.testing.assert_almost_equal(data.dmx(), np.median(RES))
    np.testing.assert_almost_equal(data.dmy(), np.median(RES))
    np.testing.assert_almost_equal(data.dmx(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dmy(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dmx(native=True), np.median(RES))
    np.testing.assert_almost_equal(data.dmy(native=True), np.median(RES))


def test_dmx_dmy_cart_engineering():
    data = PointSkeleton(x=X, y=Y)
    np.testing.assert_almost_equal(data.dmx(), np.median(RES))
    np.testing.assert_almost_equal(data.dmy(), np.median(RES))
    np.testing.assert_almost_equal(data.dmx(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dmy(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dmx(native=True), np.median(RES))
    np.testing.assert_almost_equal(data.dmy(native=True), np.median(RES))

def test_dlon_dlat_cart_engineering():
    data = PointSkeleton(x=X, y=Y)
    assert data.dlon() is None
    assert data.dlat() is None
    assert data.dlon(strict=True) is None
    assert data.dlat(strict=True) is None
    np.testing.assert_almost_equal(data.dlon(native=True), data.dmx())
    np.testing.assert_almost_equal(data.dlat(native=True), data.dmy())
    
def test_dlon_dlat_cart():
    data = PointSkeleton(x=X, y=Y, crs=CRS)
    np.testing.assert_almost_equal(data.dlat(), data.dmy()/111_000, decimal=2)
    dlon = dx_to_dlon(dx=data.dmx(), lon=data.lon()[1], lat=data.lat()[1])
    np.testing.assert_almost_equal(data.dlon(), dlon)
    assert data.dlon(strict=True) is None
    assert data.dlat(strict=True) is None
    np.testing.assert_almost_equal(data.dlon(native=True), data.dmx())
    np.testing.assert_almost_equal(data.dlat(native=True), data.dmy())