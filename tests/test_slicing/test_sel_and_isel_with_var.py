from geo_skeletons import PointSkeleton
from geo_skeletons.errors import GridError
import numpy as np
import pytest

def test_sel_hs():
    points = PointSkeleton.add_datavar('hs')(lon=(10,20), lat=(50,60))
    points.set_hs([0,5])
    pp = points.sel(hs=slice(1,10))

    np.testing.assert_almost_equal(pp.lon(), 20)
    np.testing.assert_almost_equal(pp.lat(), 60)
    np.testing.assert_almost_equal(pp.hs(),5)

def test_sel_hs_with_trivial_time():
    points = PointSkeleton.add_time().add_datavar('hs')(lon=(10,20), lat=(50,60), time='2020-01-01 00:00')
    points.set_hs([0,5])
    pp = points.sel(hs=slice(1,10))

    np.testing.assert_almost_equal(pp.lon(), 20)
    np.testing.assert_almost_equal(pp.lat(), 60)
    np.testing.assert_almost_equal(pp.hs(),5)


def test_sel_hs_with_time():
    points = PointSkeleton.add_time().add_datavar('hs')(lon=(10,20), lat=(50,60), time=('2020-01-01 00:00', '2020-01-01 01:00'))
    points.set_hs([[0,5], [0,5]])
    with pytest.raises(KeyError):
        pp = points.sel(hs=slice(1,10))

def test_sel_hs_with_time_sliced_away():
    points = PointSkeleton.add_time().add_datavar('hs')(lon=(10,20), lat=(50,60), time=('2020-01-01 00:00', '2020-01-01 01:00'))
    points.set_hs([[0,5], [0,5]])
    pp = points.isel(time=0).sel(hs=slice(1,10))
    np.testing.assert_almost_equal(pp.lon(), 20)
    np.testing.assert_almost_equal(pp.lat(), 60)
    np.testing.assert_almost_equal(pp.hs(),5)


def test_sel_hs_and_lon():
    points = PointSkeleton.add_datavar('hs')(lon=(10,20,30), lat=(50,60,70))
    points.set_hs([0,5,8])
    pp = points.sel(hs=slice(1,10), lon=slice(15,24))

    np.testing.assert_almost_equal(pp.lon(), 20)
    np.testing.assert_almost_equal(pp.lat(), 60)
    np.testing.assert_almost_equal(pp.hs(),5)

def test_sel_hs_and_lon_lat():
    points = PointSkeleton.add_datavar('hs')(lon=(10,20,30,22), lat=(50,60,70,90))
    points.set_hs([0,5,8,6])
    pp = points.sel(hs=slice(1,10), lon=slice(15,24), lat=90)

    np.testing.assert_almost_equal(pp.lon(), 22)
    np.testing.assert_almost_equal(pp.lat(), 90)
    np.testing.assert_almost_equal(pp.hs(),6)

def test_sel_hs_and_lon_lat_with_list():
    points = PointSkeleton.add_datavar('hs')(lon=(10,20,30,22), lat=(50,60,70,90))
    points.set_hs([0,5,8,6])
    pp = points.sel(hs=slice(1,10), lon=[20,22], lat=90)

    np.testing.assert_almost_equal(pp.lon(), 22)
    np.testing.assert_almost_equal(pp.lat(), 90)
    np.testing.assert_almost_equal(pp.hs(),6)


def test_slice_down_to_empty():
    points = PointSkeleton.add_datavar('hs')(lon=(10,20,30,22), lat=(50,60,70,90))
    points.set_hs([0,5,8,6])
    with pytest.raises(GridError):
        pp = points.sel(hs=slice(10,20), lon=[20,22], lat=90)

