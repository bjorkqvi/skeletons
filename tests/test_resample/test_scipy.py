from geo_skeletons.classes import WindGrid, Wind
from geo_skeletons import GriddedSkeleton, PointSkeleton
import numpy as np

from copy import deepcopy
def test_scipy_grid_grid():
    data = WindGrid.add_time()(
        lon=(10, 20), lat=(50, 60), time=("2020-01-01 00:00", "2020-01-10 06:00")
    )
    data.set_spacing(nx=11, ny=21)
    data.set_u(10)
    data.set_v(5)

    new_grid = GriddedSkeleton(lon=(10.1, 20.1), lat=(50.1, 60.1))
    new_grid.set_spacing(nx=11, ny=21)

    new_data = data.resample.grid(new_grid)

    assert new_data.is_gridded()
    np.testing.assert_array_almost_equal(new_data.lon(), new_grid.lon())
    np.testing.assert_array_almost_equal(new_data.lat(), new_grid.lat())
    assert new_data.time(datetime=False) == data.time(datetime=False)
    np.testing.assert_array_almost_equal(new_data.u(), data.u())
    np.testing.assert_array_almost_equal(new_data.v(), data.v())

def test_nan_treatment():
    data = Wind.add_time()(lon=(10,11,12,20), lat=(50,50,51,60), time=('2020-01-01 00:00', '2020-01-10 06:00'))
    data_high = Wind.add_time()(lon=(10,11,12,20), lat=(50,50,51,60), time=('2020-01-01 00:00', '2020-01-10 06:00'))
    data_nan = Wind.add_time()(lon=(10,11,12,20), lat=(50,50,51,60), time=('2020-01-01 00:00', '2020-01-10 06:00'))
    
    data.set_dd(0)
    data.set_ff(10)
    
    u = deepcopy(data.u())
    v= deepcopy(data.v())
    u[:,0] = 100
    v[:,0] = 100
    data_high.set_u(u)
    data_high.set_v(v)

    u[:,0] = np.nan
    v[:,0] = np.nan

    data_nan.set_u(u)
    data_nan.set_v(v)

    grid = Wind(x=data.edges('x'),y=data.edges('y'), utm=data.utm.zone())
    #grid.set_spacing(nx=10, ny=21)

    new_data = data.resample.grid(grid)
    new_data_high = data_high.resample.grid(grid)
    new_data_high_from_nan = data_nan.resample.grid(grid, mask_nan=100)
    new_data_drop_nan = data_nan.resample.grid(grid, drop_nan=True)

    np.testing.assert_array_almost_equal(new_data_high.u(), new_data_high_from_nan.u())
    np.testing.assert_array_almost_equal(new_data_high.v(), new_data_high_from_nan.v())
    np.testing.assert_array_almost_equal(new_data.u(), new_data_drop_nan.u())
    np.testing.assert_array_almost_equal(new_data.v(), new_data_drop_nan.v())
