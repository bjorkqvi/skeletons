from geo_skeletons.gridded_skeleton import GriddedSkeleton
from geo_skeletons.decorators import add_coord, add_time, add_datavar

import numpy as np
import pandas as pd


def test_add_z_and_time_coord():
    @add_datavar(name="hs", default_value=0.0, coords="all")
    @add_time(grid_coord=False)
    @add_coord(grid_coord=True, name="z")
    class TimeSeries(GriddedSkeleton):
        pass

    times = pd.date_range("2018-01-01 00:00", "2018-02-01 00:00", freq="1H")
    ts = TimeSeries(x=(0.0, 1.0), y=(10.0, 20.0), time=times, z=(10, 20))
    ts.set_spacing(nx=5, ny=6)
    ts.set_z_spacing(nx=11)

    np.testing.assert_array_almost_equal(
        ts.x(x=slice(0.25, 0.75)), np.array([0.25, 0.5, 0.75])
    )
    np.testing.assert_array_almost_equal(ts.y(y=slice(12, 14)), np.array([12, 14]))
    np.testing.assert_array_almost_equal(ts.z(z=slice(12, 14)), np.array([12, 13, 14]))

    assert ts.hs().shape == (len(ts.time()), len(ts.y()), len(ts.x()), len(ts.z()))
    assert ts.hs(x=0).shape == (len(ts.time()), len(ts.y()), len(ts.z()))
    assert ts.hs(y=10).shape == (len(ts.time()), len(ts.x()), len(ts.z()))
    assert ts.hs(z=10).shape == (len(ts.time()), len(ts.y()), len(ts.x()))
    assert ts.hs(x=0, y=10).shape == (len(ts.time()), len(ts.z()))
    assert ts.hs(x=0, y=10.01, method="nearest").shape == (len(ts.time()), len(ts.z()))
    assert ts.hs(
        x=0, y=10, time=slice("2018-01-01 01:00", "2018-01-01 12:00")
    ).shape == (12, len(ts.z()))
