from geo_skeletons import GriddedSkeleton
import pytest
import numpy as np
def test_get_xy_lonlat():
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).x(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).y(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).lon(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).lat(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).xy(strict=True, native=True)
    with pytest.raises(ValueError):
        GriddedSkeleton(lon=1, lat=1).lonlat(strict=True, native=True)

def test_dx_dy():
    data = GriddedSkeleton(x=(0,2), y=(4,8))
    data.set_spacing(dx=1, dy=2)
    np.testing.assert_almost_equal(data.dx(), 1)
    np.testing.assert_almost_equal(data.dy(), 2)
    np.testing.assert_almost_equal(data.dx(strict=True), 1)
    np.testing.assert_almost_equal(data.dy(strict=True), 2)
    np.testing.assert_almost_equal(data.dx(native=True), 1)
    np.testing.assert_almost_equal(data.dy(native=True), 2)

def test_dmx_dmy():
    data = GriddedSkeleton(x=(0,2), y=(4,8))
    data.set_spacing(dx=1, dy=2)
    np.testing.assert_almost_equal(data.dmx(), 1)
    np.testing.assert_almost_equal(data.dmy(), 2)
    np.testing.assert_almost_equal(data.dmx(strict=True), 1)
    np.testing.assert_almost_equal(data.dmy(strict=True), 2)
    np.testing.assert_almost_equal(data.dmx(native=True), 1)
    np.testing.assert_almost_equal(data.dmy(native=True), 2)


def test_dlon_dlat():
    data = GriddedSkeleton(x=(0,2), y=(4,8))
    data.set_spacing(dx=1, dy=2)
    assert data.dlon() is None
    assert data.dlat() is None
    assert data.dlon(strict=True) is None
    assert data.dlat(strict=True) is None
    np.testing.assert_almost_equal(data.dlon(native=True), 1)
    np.testing.assert_almost_equal(data.dlat(native=True), 2)