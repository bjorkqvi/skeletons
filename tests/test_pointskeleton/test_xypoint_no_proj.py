from geo_skeletons import PointSkeleton
import pytest
import numpy as np


def test_resolution():
    data = PointSkeleton(x=(0,1,0), y=(0,1,2))
    np.testing.assert_almost_equal(data.resolution(), np.array([2**0.5, 2**0.5, 2**0.5]))

def test_dx_dy():
    data = PointSkeleton(x=(0,1,0), y=(0,1,2))
    np.testing.assert_almost_equal(data.dx(), 2**0.5)
    np.testing.assert_almost_equal(data.dy(), 2**0.5)
    np.testing.assert_almost_equal(data.dx(strict=True), data.dx())
    np.testing.assert_almost_equal(data.dx(native=True), data.dx())
    np.testing.assert_almost_equal(data.dy(strict=True), data.dy())
    np.testing.assert_almost_equal(data.dy(native=True), data.dy())

def test_dmx_dmy():
    data = PointSkeleton(x=(0,1,0), y=(0,1,2))
    np.testing.assert_almost_equal(data.dmx(), 2**0.5)
    np.testing.assert_almost_equal(data.dmy(), 2**0.5)
    np.testing.assert_almost_equal(data.dmx(strict=True), data.dmx())
    np.testing.assert_almost_equal(data.dmx(native=True), data.dmx())
    np.testing.assert_almost_equal(data.dmy(strict=True), data.dmy())
    np.testing.assert_almost_equal(data.dmy(native=True), data.dmy())

def test_dlon_dlat():
    data = PointSkeleton(x=(0,1,0), y=(0,1,2))
    
    assert data.dlon() is None
    assert data.dlat() is None

    assert data.dlon(strict=True) is None
    np.testing.assert_almost_equal(data.dlon(native=True), data.dmx())
    assert data.dlat(strict=True) is None
    np.testing.assert_almost_equal(data.dlat(native=True), data.dmy())