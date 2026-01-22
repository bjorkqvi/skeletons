from geo_skeletons import PointSkeleton
import pytest
import numpy as np
def test_get_xy_lonlat():
    with pytest.raises(ValueError):
        PointSkeleton(lon=1, lat=1).x(strict=True, native=True)
    with pytest.raises(ValueError):
        PointSkeleton(lon=1, lat=1).y(strict=True, native=True)
    with pytest.raises(ValueError):
        PointSkeleton(lon=1, lat=1).lon(strict=True, native=True)
    with pytest.raises(ValueError):
        PointSkeleton(lon=1, lat=1).lat(strict=True, native=True)
    with pytest.raises(ValueError):
        PointSkeleton(lon=1, lat=1).xy(strict=True, native=True)
    with pytest.raises(ValueError):
        PointSkeleton(lon=1, lat=1).lonlat(strict=True, native=True)

def test_dxy_cartesian():
    data = PointSkeleton(x=(0,1,0), y=(0,1,2))
    np.testing.assert_almost_equal(data.dx(), 2**0.5)
    np.testing.assert_almost_equal(data.dy(), 2**0.5)
    np.testing.assert_almost_equal(data.dx(strict=True), data.dx())
    np.testing.assert_almost_equal(data.dx(native=True), data.dx())
    np.testing.assert_almost_equal(data.dy(strict=True), data.dy())
    np.testing.assert_almost_equal(data.dy(native=True), data.dy())

    data = PointSkeleton(x=(0), y=(0))
    np.testing.assert_almost_equal(data.dx(), 0)
    np.testing.assert_almost_equal(data.dy(), 0)


def test_dxy_spherical():
    data = PointSkeleton(lon=(0,0,0), lat=(0,1/60,2/60))
    np.testing.assert_almost_equal(data.dx(), 1845, decimal=0)
    np.testing.assert_almost_equal(data.dy(), 1845, decimal=0)
    assert data.dx(strict=True) is None
    np.testing.assert_almost_equal(data.dx(native=True), 1/60, decimal=4)
    assert data.dy(strict=True) is None
    np.testing.assert_almost_equal(data.dy(native=True), 1/60, decimal=4)

    data = PointSkeleton(lat=(0), lon=(0))
    np.testing.assert_almost_equal(data.dx(), 0)
    np.testing.assert_almost_equal(data.dy(), 0)



def test_dlonlat_spherical():
    data = PointSkeleton(lon=(0,0,0), lat=(0,1/60,2/60))
    np.testing.assert_almost_equal(data.dlon(), 1/60, decimal=4)
    np.testing.assert_almost_equal(data.dlat(), 1/60, decimal=4)
    np.testing.assert_almost_equal(data.dlon(strict=True), data.dlon())
    np.testing.assert_almost_equal(data.dlon(native=True), data.dlon())
    np.testing.assert_almost_equal(data.dlat(strict=True), data.dlat())
    np.testing.assert_almost_equal(data.dlat(native=True), data.dlat())
    data = PointSkeleton(lat=(0), lon=(0))
    np.testing.assert_almost_equal(data.dlon(), 0)
    np.testing.assert_almost_equal(data.dlat(), 0)

def test_dlonlat_cartesian():
    data0 = PointSkeleton(lon=(0,0,0), lat=(0,1/60,2/60))
    data = PointSkeleton(x=data0.x(), y=data0.y(), crs=data0.proj.crs())
    np.testing.assert_almost_equal(data.dlon(), 1/60, decimal=4)
    np.testing.assert_almost_equal(data.dlat(), 1/60, decimal=4)

    assert data.dlon(strict=True) is None
    np.testing.assert_almost_equal(data.dlon(native=True), 1845, decimal=0)
    assert data.dlat(strict=True) is None
    np.testing.assert_almost_equal(data.dlat(native=True), 1845, decimal=0)
    data = PointSkeleton(x=(0), y=(0))
    assert data.dlon() is None
    assert data.dlat() is None
