from geo_skeletons.gridded_skeleton import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_coord, add_time, add_datavar

import numpy as np
import pandas as pd


def test_add_z_and_time_coord():
    @add_datavar(name="hs", default_value=0.0, coord_group="all")
    @add_time(grid_coord=False)
    @add_coord(grid_coord=True, name="z")
    class TimeSeries(GriddedSkeleton):
        pass

    times = pd.date_range("2018-01-01 00:00", "2018-02-01 00:00", freq="1h")
    ts = TimeSeries(x=(0.0, 1.0), y=(10.0, 20.0), time=times, z=(10, 20))
    ts.set_spacing(nx=5, ny=6)
    ts.set_z_spacing(nx=11)

    np.testing.assert_array_almost_equal(
        ts.x(x=slice(0.25, 0.75)), np.array([0.25, 0.5, 0.75])
    )
    np.testing.assert_array_almost_equal(ts.y(y=slice(12, 14)), np.array([12, 14]))
    np.testing.assert_array_almost_equal(ts.z(z=slice(12, 14)), np.array([12, 13, 14]))
    ts.set_hs(0)
    assert ts.hs().shape == (len(ts.time()), len(ts.y()), len(ts.x()), len(ts.z()))
    assert ts.hs(x=0).shape == (len(ts.time()), len(ts.y()), len(ts.z()))
    assert ts.hs(y=10).shape == (len(ts.time()), len(ts.x()), len(ts.z()))
    assert ts.hs(z=10).shape == (len(ts.time()), len(ts.y()), len(ts.x()))
    assert ts.hs(x=0, y=10).shape == (len(ts.time()), len(ts.z()))
    assert ts.hs(x=0, y=10.01, method="nearest").shape == (len(ts.time()), len(ts.z()))
    assert ts.hs(
        x=0, y=10, time=slice("2018-01-01 01:00", "2018-01-01 12:00")
    ).shape == (12, len(ts.z()))


def test_added_trivial_dim():
    @add_datavar(name="hs", default_value=0.0, coord_group="all")
    @add_coord(grid_coord=True, name="z")
    class Expanded(GriddedSkeleton):
        pass

    data = Expanded(lon=(1, 2, 4), lat=(5, 6), z=0)
    data.set_hs(1)

    assert data.hs(z=0).shape == (2, 3)


def test_slice_down_to_single_value():
    @add_datavar(name="hs", default_value=0.0, coord_group="all")
    @add_coord(grid_coord=True, name="z")
    class Expanded(PointSkeleton):
        pass

    data = Expanded(lon=(1, 2, 4), lat=(5, 6, 7), z=0)
    data.set_hs(1)

    assert data.hs(z=0, inds=0).shape == (1,)


def test_slice_down_to_single_value_gridded():
    @add_datavar(name="hs", default_value=0.0, coord_group="all")
    @add_coord(grid_coord=True, name="z")
    class Expanded(GriddedSkeleton):
        pass

    data = Expanded(lon=(1, 2, 4), lat=(5, 6), z=0)
    data.set_hs(1)

    assert data.hs(z=0, lon=1).shape == (2,)
    assert data.hs(z=0, lon=1, lat=6).shape == (1,1)

def test_point_slice_one_point_to_trivial_dimension():
    @add_datavar('hs')
    @add_coord('quantile')
    @add_time()
    class Quantiles(PointSkeleton):
        pass

    data = Quantiles(x=1, y=2, time=["2020-05-01 00:00"], quantile=[0.1,0.5,0.9])
    data.set_hs([1,5,9])
    assert data.hs().shape == (3,)
    assert data.hs(squeeze=False).shape == (1,1,3)

    assert data.hs(quantile=0.1, squeeze=False).shape == (1,1,1)
    assert data.hs(quantile=0.1).shape == (1,)

def test_point_slice_one_point_to_trivial_dimension_with_two_coordinates():
    @add_datavar('hs')
    @add_coord('member')
    @add_coord('quantile')
    @add_time()
    class Quantiles(PointSkeleton):
        pass

    data = Quantiles(x=1, y=2, time=["2020-05-01 00:00"], quantile=[0.1,0.5,0.9], member=[0,1])
    data.set_hs([[1,5,9],[2,6,10]], coords=['member','quantile'])

    assert data.hs().shape == (3,2)
    assert data.hs(squeeze=False).shape == (1,1,3,2)

    assert data.hs(quantile=0.1, squeeze=False).shape == (1,1,1,2)
    assert data.hs(quantile=0.1).shape == (2,)
    
    assert data.hs(quantile=0.1, member=0, squeeze=False).shape == (1,1,1,1)
    assert data.hs(quantile=0.1, member=0).shape == (1,)


def test_gridded_slice_one_point_to_trivial_dimension():
    @add_datavar('hs')
    @add_coord('quantile')
    @add_time()
    class Quantiles(GriddedSkeleton):
        pass

    data = Quantiles(x=1, y=2, time=["2020-05-01 00:00"], quantile=[0.1,0.5,0.9])
    data.set_hs([1,5,9])
    assert data.hs().shape == (3,)
    assert data.hs(squeeze=False).shape == (1,1,1,3)

    assert data.hs(quantile=0.1, squeeze=False).shape == (1,1,1,1)
    assert data.hs(quantile=0.1).shape == (1,1)

def test_gridded_slice_one_point_to_trivial_dimension_with_two_coordinates():
    @add_datavar('hs')
    @add_coord('member')
    @add_coord('quantile')
    @add_time()
    class Quantiles(GriddedSkeleton):
        pass

    data = Quantiles(x=1, y=2, time=["2020-05-01 00:00"], quantile=[0.1,0.5,0.9], member=[0,1])
    data.set_hs([[1,5,9],[2,6,10]], coords=['member','quantile'])

    assert data.hs().shape == (3,2)
    assert data.hs(squeeze=False).shape == (1,1,1,3,2)

    assert data.hs(quantile=0.1, squeeze=False).shape == (1,1,1,1,2)
    assert data.hs(quantile=0.1).shape == (2,)
    
    assert data.hs(quantile=0.1, member=0, squeeze=False).shape == (1,1,1,1,1)
    assert data.hs(quantile=0.1, member=0).shape == (1,1)