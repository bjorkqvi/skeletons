from geo_skeletons import PointSkeleton

import numpy as np
from geo_skeletons.distance_funcs import dx_to_dlon, get_dist_point, get_dist_point_old

LON = (20,21,22)
LAT = (58,59,60)
X = (440892.10517494, 500000.        , 555776.26675182)
Y = (6429147.61178793, 6540052.01822338, 6651832.73615611)
CRS = (34, 'V')
RES = get_dist_point(np.array(X).astype(int),np.array(Y).astype(int))


def test_resolution():
    data = PointSkeleton(lon=LON, lat=LAT)
    np.testing.assert_almost_equal(data.resolution(), RES)

def test_dx_dy():
    data = PointSkeleton(lon=LON, lat=LAT)
    np.testing.assert_almost_equal(data.dx(), np.median(RES))
    np.testing.assert_almost_equal(data.dy(), np.median(RES))
    assert data.dx(strict=True) is None
    dlon = dx_to_dlon(dx=data.dmx(), lon=data.lon()[1], lat=data.lat()[1])
    np.testing.assert_almost_equal(data.dx(native=True), dlon)
    assert data.dy(strict=True) is None
    np.testing.assert_almost_equal(data.dy(native=True), data.dmy()/111_000, decimal=2)

def test_dmx_dmy():
    data = PointSkeleton(lon=LON, lat=LAT)
    np.testing.assert_almost_equal(data.dmx(), np.median(RES))
    np.testing.assert_almost_equal(data.dmy(), np.median(RES))
    assert data.dmx(strict=True) is None
    dlon = dx_to_dlon(dx=data.dmx(), lon=data.lon()[1], lat=data.lat()[1])
    np.testing.assert_almost_equal(data.dmx(native=True), dlon)
    assert data.dmy(strict=True) is None
    np.testing.assert_almost_equal(data.dmy(native=True), data.dmy()/111_000, decimal=2)

def test_dlon_dlat():
    data = PointSkeleton(lon=LON, lat=LAT)
    np.testing.assert_almost_equal(data.dlat(), data.dmy()/111_000, decimal=2)
    dlon = dx_to_dlon(dx=data.dmx(), lon=data.lon()[1], lat=data.lat()[1])
    np.testing.assert_almost_equal(data.dlon(), dlon)
    np.testing.assert_almost_equal(data.dlon(strict=True), dlon)
    np.testing.assert_almost_equal(data.dlon(native=True), dlon)
    np.testing.assert_almost_equal(data.dlat(strict=True), data.dmy()/111_000, decimal=2)
    np.testing.assert_almost_equal(data.dlat(native=True), data.dmy()/111_000, decimal=2)


def test_resolution_cart():
    data = PointSkeleton(x=X, y=Y)
    np.testing.assert_almost_equal(data.resolution(), RES)

def test_dx_dy_cart():
    data = PointSkeleton(x=X, y=Y)
    np.testing.assert_almost_equal(data.dx(), np.median(RES))
    np.testing.assert_almost_equal(data.dy(), np.median(RES))
    assert data.dlon(strict=True) is None
    np.testing.assert_almost_equal(data.dx(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dx(native=True), np.median(RES))
    assert data.dlon(strict=True) is None
    np.testing.assert_almost_equal(data.dy(native=True), data.dmy(), decimal=2)
    np.testing.assert_almost_equal(data.dy(), data.dmy(), decimal=2)

def test_dmx_dmy_cart():
    data = PointSkeleton(x=X, y=Y, crs=CRS)
    np.testing.assert_almost_equal(data.dmx(), np.median(RES))
    np.testing.assert_almost_equal(data.dmy(), np.median(RES))
    np.testing.assert_almost_equal(data.dmx(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dmy(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dlon(native=True), np.median(RES))
    np.testing.assert_almost_equal(data.dlat(native=True), np.median(RES))
    assert data.dlat(strict=True) is None
    assert data.dlon(strict=True) is None
    dlon = dx_to_dlon(dx=data.dmx(), lon=data.lon()[1], lat=data.lat()[1])
    np.testing.assert_almost_equal(data.dlon(), dlon)
    np.testing.assert_almost_equal(data.dlat(), data.dmy()/111_000, decimal=2)

def test_dlon_dlat_cart():
    data = PointSkeleton(x=X, y=Y)
    np.testing.assert_almost_equal(data.dlat(), data.dmy()/111_000, decimal=2)
    dlon = dx_to_dlon(dx=data.dmx(), lon=data.lon()[1], lat=data.lat()[1])
    np.testing.assert_almost_equal(data.dlon(), dlon)
    np.testing.assert_almost_equal(data.dlon(strict=True), dlon)
    np.testing.assert_almost_equal(data.dlon(native=True), dlon)
    np.testing.assert_almost_equal(data.dlat(strict=True), data.dmy()/111_000, decimal=2)
    np.testing.assert_almost_equal(data.dlat(native=True), data.dmy()/111_000, decimal=2)