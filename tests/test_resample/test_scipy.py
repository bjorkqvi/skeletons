from geo_skeletons.classes import WindGrid, Wind
from geo_skeletons import GriddedSkeleton, PointSkeleton
import numpy as np


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
