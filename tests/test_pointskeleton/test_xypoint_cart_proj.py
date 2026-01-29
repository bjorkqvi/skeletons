from geo_skeletons import PointSkeleton

import numpy as np
from geo_skeletons.distance_funcs import dx_to_dlon

X = (440892.10517494264, 500000, 559107.8948250574)
Y = (6429147.6117879255, 6500000, 6651832.7361561125)
CRS = (34, 'V')
RES = np.array([92270.27769549932, 92270.27769549932, 162932.2650652771])

def test_resolution():
    data = PointSkeleton(x=X, y=Y, crs=CRS)
    np.testing.assert_almost_equal(data.resolution(), RES)

def test_dx_dy():
    data = PointSkeleton(x=X, y=Y, crs=CRS)
    np.testing.assert_almost_equal(data.dx(), np.median(RES))
    np.testing.assert_almost_equal(data.dy(), np.median(RES))
    np.testing.assert_almost_equal(data.dx(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dx(native=True), np.median(RES))
    np.testing.assert_almost_equal(data.dy(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dy(native=True), np.median(RES))

def test_dmx_dmy():
    data = PointSkeleton(x=X, y=Y, crs=CRS)
    np.testing.assert_almost_equal(data.dmx(), np.median(RES))
    np.testing.assert_almost_equal(data.dmy(), np.median(RES))
    np.testing.assert_almost_equal(data.dmx(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dmx(native=True), np.median(RES))
    np.testing.assert_almost_equal(data.dmy(strict=True), np.median(RES))
    np.testing.assert_almost_equal(data.dmy(native=True), np.median(RES))

def test_dlon_dlat():
    data = PointSkeleton(x=X, y=Y, crs=CRS)
    np.testing.assert_almost_equal(data.dlat(), data.dmy()/111_000, decimal=2)
    dlon = dx_to_dlon(dx=data.dmx(), lon=data.lon()[1], lat=data.lat()[1])
    np.testing.assert_almost_equal(data.dlon(), dlon)
    assert data.dlon(strict=True) is None
    np.testing.assert_almost_equal(data.dlon(native=True), np.median(RES))
    assert data.dlat(strict=True) is None
    np.testing.assert_almost_equal(data.dlat(native=True), np.median(RES))