from ..point_skeleton import PointSkeleton
from ..coordinate_factory import add_coord, add_time
from ..datavar_factory import add_datavar
from ..mask_factory import add_mask
import numpy as np
import pandas as pd


def test_add_mask():
    @add_mask(name="sea", default_value=1.0, opposite_name="land")
    @add_datavar(name="hs", default_value=0)
    class WaveHeight(PointSkeleton):
        pass

    data = WaveHeight(lon=(10, 20), lat=(30, 40))
    np.testing.assert_array_equal(data.sea_mask(), np.full(data.size(), True))
    np.testing.assert_array_equal(data.land_mask(), np.full(data.size(), False))
    data.set_sea_mask(data.hs() > 0)
    np.testing.assert_array_equal(data.sea_mask(), np.full(data.size(), False))
    np.testing.assert_array_equal(data.land_mask(), np.full(data.size(), True))


def test_add_coord_and_mask():
    @add_mask(name="sea", default_value=1.0, opposite_name="land")
    @add_datavar(name="hs", default_value=0.0)
    @add_coord(name="z", grid_coord=True)
    class WaveHeight(PointSkeleton):
        pass

    data = WaveHeight(lon=(10, 20), lat=(30, 40), z=(1, 2, 3))
    np.testing.assert_array_equal(data.sea_mask(), np.full(data.size(), True))
    np.testing.assert_array_equal(data.land_mask(), np.full(data.size(), False))
    data.set_sea_mask(data.hs() > 0)
    np.testing.assert_array_equal(data.sea_mask(), np.full(data.size(), False))
    np.testing.assert_array_equal(data.land_mask(), np.full(data.size(), True))


def test_add_gridpoint_coord_and_mask():
    @add_mask(name="sea", default_value=1.0, opposite_name="land", coords="grid")
    @add_datavar(name="hs", default_value=0.0)
    @add_time(name="time", grid_coord=False)
    @add_coord(name="z", grid_coord=True)
    class WaveHeight(PointSkeleton):
        pass

    times = pd.date_range("2018-01-01 00:00", "2018-02-01 00:00", freq="1H")
    data = WaveHeight(lon=(10, 20), lat=(30, 40), z=(1, 2, 3), time=times)
    np.testing.assert_array_equal(
        data.sea_mask(), np.full(data.size(coords="grid"), True)
    )
    np.testing.assert_array_equal(
        data.land_mask(), np.full(data.size(coords="grid"), False)
    )
    data.set_sea_mask(data.hs()[0, :] > 0)
    np.testing.assert_array_equal(
        data.sea_mask(), np.full(data.size(coords="grid"), False)
    )
    np.testing.assert_array_equal(
        data.land_mask(), np.full(data.size(coords="grid"), True)
    )


def test_get_points():
    @add_mask(
        name="sea",
        default_value=1.0,
        opposite_name="land",
    )
    @add_datavar(name="hs", default_value=0)
    class WaveHeight(PointSkeleton):
        pass

    data = WaveHeight(lon=(10, 20, 30), lat=(30, 40, 50))
    mask = data.sea_mask()

    lon, lat = data.sea_points()
    np.testing.assert_array_almost_equal(lon, np.array([10, 20, 30]))
    np.testing.assert_array_almost_equal(lat, np.array([30, 40, 50]))

    lon, lat = data.land_points()
    np.testing.assert_array_almost_equal(lon, np.array([]))
    np.testing.assert_array_almost_equal(lat, np.array([]))

    mask[0] = False
    data.set_sea_mask(mask)
    lon, lat = data.sea_points()
    np.testing.assert_array_almost_equal(lon, np.array([20, 30]))
    np.testing.assert_array_almost_equal(lat, np.array([40, 50]))

    lon, lat = data.land_points()
    np.testing.assert_array_almost_equal(lon, np.array([10]))
    np.testing.assert_array_almost_equal(lat, np.array([30]))
