from geo_skeletons import PointSkeleton, GriddedSkeleton
import geo_parameters as gp
import numpy as np
import pytest
X = (570168.8615105285, 643329.123886088)
Y = (5539109.815668365, 5651728.683075359)
UTM = (32, 'U')
def test_filter_gridded():
    cls = GriddedSkeleton.add_datavar('hs')
    data= cls(lon=(10,11), lat=(50,51))
    data.set_spacing(dlon=0.1, dlat=0.1)
    hs = np.random.rand(*data.shape('hs'))
    hs_orig = hs.copy()
    data.set_hs(hs)
    data2 = data.resample.filter(dlon=0.3, dlat=0.3)
    data3 = data.resample.filter(nx=3, ny=3)
    np.testing.assert_array_almost_equal(data2.hs(), data3.hs())
    np.testing.assert_array_almost_equal(data.hs(), hs_orig)


def test_filter_gridded_cart():
    cls = GriddedSkeleton.add_datavar('hs')
    data= cls(x=X, y=Y, utm=UTM)
    data.set_spacing(dmx=3000, dmy=3000)
    hs = np.random.rand(*data.shape('hs'))
    hs_orig = hs.copy()
    data.set_hs(hs)
    data2 = data.resample.filter(dmx=9000, dmy=9000)
    data3 = data.resample.filter(nx=3, ny=3)
    data4 = data.resample.filter(dm=9000)
    np.testing.assert_array_almost_equal(data2.hs(), data3.hs())
    np.testing.assert_array_almost_equal(data2.hs(), data4.hs())
    np.testing.assert_array_almost_equal(data.hs(), hs_orig)

def test_filter_gridded_select_var():
    cls = GriddedSkeleton.add_datavar(['hs','tp'])
    data= cls(lon=(10,11), lat=(50,51))
    data.set_spacing(dlon=0.1, dlat=0.1)
    hs = np.random.rand(*data.shape('hs'))
    hs_orig = hs.copy()
    data.set_hs(hs)
    data.set_tp(hs)
    data2 = data.resample.filter(dlon=0.3, dlat=0.3, data_vars=['hs'])
    data3 = data.resample.filter(nx=3, ny=3, data_vars=['hs'])
    np.testing.assert_array_almost_equal(data2.hs(), data3.hs())
    np.testing.assert_array_almost_equal(data.hs(), hs_orig)
    np.testing.assert_array_almost_equal(data2.tp(), data.tp())
    np.testing.assert_array_almost_equal(data3.tp(), data.tp())

def test_filter_gridded_select_var_drop_var():
    cls = GriddedSkeleton.add_datavar(['hs','tp'])
    data= cls(lon=(10,11), lat=(50,51))
    data.set_spacing(dlon=0.1, dlat=0.1)
    hs = np.random.rand(*data.shape('hs'))
    hs_orig = hs.copy()
    data.set_hs(hs)
    data.set_tp(hs)
    data2 = data.resample.filter(dlon=0.3, dlat=0.3, data_vars=['hs'],drop_vars=True)
    data3 = data.resample.filter(nx=3, ny=3, data_vars=['hs'],drop_vars=True)
    np.testing.assert_array_almost_equal(data2.hs(), data3.hs())
    np.testing.assert_array_almost_equal(data.hs(), hs_orig)
    assert data2.tp() is None
    assert data3.tp() is None
    

def test_filter_gridded_select_var_with_gp():
    cls = GriddedSkeleton.add_datavar([gp.wave.Hs('hs'),gp.wave.Tp('tp')])
    data= cls(lon=(10,11), lat=(50,51))
    data.set_spacing(dlon=0.1, dlat=0.1)
    hs = np.random.rand(*data.shape('hs'))
    hs_orig = hs.copy()
    data.set_hs(hs)
    data.set_tp(hs)
    data2 = data.resample.filter(dlon=0.3, dlat=0.3, data_vars=[gp.wave.Hs])
    data3 = data.resample.filter(nx=3, ny=3, data_vars=[gp.wave.Hs])
    np.testing.assert_array_almost_equal(data2.hs(), data3.hs())
    np.testing.assert_array_almost_equal(data.hs(), hs_orig)
    np.testing.assert_array_almost_equal(data2.tp(), data.tp())
    np.testing.assert_array_almost_equal(data3.tp(), data.tp())

def test_filter_gridded_time():
    cls = GriddedSkeleton.add_time().add_datavar('hs')
    data= cls(lon=(10,11), lat=(50,51), time=('2020-01-01 00:00', '2020-01-03 00:00'))
    data.set_spacing(dlon=0.1, dlat=0.1)
    hs = np.random.rand(*data.shape('hs'))
    hs_orig = hs.copy()
    data.set_hs(hs)
    data2 = data.resample.filter(dlon=0.3, dlat=0.3, dt=0)
    data_one_time = data.isel(time=10).resample.filter(dlon=0.3, dlat=0.3)
    data3 = data.resample.filter(nx=3, ny=3, nt=3)
    data4 = data.resample.filter(nx=3, ny=3, dt='3h')
    with pytest.raises(ValueError):
        data4 = data.resample.filter(nx=3, ny=3, dt=3)

    np.testing.assert_array_almost_equal(data_one_time.hs(), data2.hs()[10,:,:])
    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(data_one_time.hs(), data3.hs()[10,:,:])
    np.testing.assert_array_almost_equal(data.hs(), hs_orig)
    np.testing.assert_array_almost_equal(data3.hs(), data4.hs())


def test_filter_point():
    cls = PointSkeleton.add_datavar('hs')
    data= cls(lon=(10,11,12), lat=(50,51,52))
    hs = np.random.rand(*data.shape('hs'))
    hs_orig = hs.copy()
    data.set_hs(hs)
    with pytest.raises(ValueError):
        data2 = data.resample.filter(dlon=0.3, dlat=0.3)
    with pytest.raises(ValueError):
        data3 = data.resample.filter(nx=3, ny=3)

def test_filter_point_time():
    cls = PointSkeleton.add_time().add_datavar('hs')
    data= cls(lon=(10,11,12), lat=(50,51,52), time=('2020-01-01 00:00', '2020-01-03 00:00'))
    hs = np.random.rand(*data.shape('hs'))
    hs_orig = hs.copy()
    data.set_hs(hs)
    with pytest.raises(ValueError):
        data2 = data.resample.filter(dlon=0.3, dlat=0.3, dt=0)
    
    data3 = data.resample.filter(nt=3)
    data4 = data.resample.filter(dt='3h')
    
    with pytest.raises(AssertionError):
        np.testing.assert_array_almost_equal(data.hs(), data3.hs())
    np.testing.assert_array_almost_equal(data3.hs(), data4.hs())
    np.testing.assert_array_almost_equal(data.hs(), hs_orig)