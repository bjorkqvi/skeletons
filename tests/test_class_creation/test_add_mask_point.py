from geo_skeletons.point_skeleton import PointSkeleton
from geo_skeletons.decorators import add_coord, add_time, add_datavar, add_mask
import numpy as np
import pandas as pd


def test_add_mask_one_point():
    @add_mask(name="sea", default_value=1.0, opposite_name="land")
    @add_datavar(name="hs", default_value=0)
    class WaveHeight(PointSkeleton):
        pass

    data = WaveHeight(lon=(10), lat=(30))
    data.set_sea_mask()
    data.set_hs()
    np.testing.assert_array_equal(data.sea_mask(), np.full(data.size(), True))
    np.testing.assert_array_equal(data.land_mask(), np.full(data.size(), False))
    assert data.sea_mask(empty=True).shape == data.size()
    data.set_sea_mask(data.hs() > 0)
    np.testing.assert_array_equal(data.sea_mask(), np.full(data.size(), False))
    np.testing.assert_array_equal(data.land_mask(), np.full(data.size(), True))
    assert data.land_mask(empty=True).shape == data.size()
    assert data.sea_mask(empty=True).shape == data.size()


def test_add_mask():
    @add_mask(name="sea", default_value=1.0, opposite_name="land")
    @add_datavar(name="hs", default_value=0)
    class WaveHeight(PointSkeleton):
        pass

    data = WaveHeight(lon=(10, 20), lat=(30, 40))
    data.set_sea_mask()
    data.set_hs()
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
    data.set_sea_mask()
    data.set_hs()
    np.testing.assert_array_equal(data.sea_mask(), np.full(data.size(), True))
    np.testing.assert_array_equal(data.land_mask(), np.full(data.size(), False))
    data.set_sea_mask(data.hs() > 0)
    np.testing.assert_array_equal(data.sea_mask(), np.full(data.size(), False))
    np.testing.assert_array_equal(data.land_mask(), np.full(data.size(), True))


def test_add_gridpoint_coord_and_mask():
    @add_mask(name="sea", default_value=1.0, opposite_name="land", coord_group="grid")
    @add_datavar(name="hs", default_value=0.0)
    @add_time(grid_coord=False)
    @add_coord(name="z", grid_coord=True)
    class WaveHeight(PointSkeleton):
        pass

    times = pd.date_range("2018-01-01 00:00", "2018-02-01 00:00", freq="1h")
    data = WaveHeight(lon=(10, 20), lat=(30, 40), z=(1, 2, 3), time=times)
    data.set_land_mask(0)
    data.set_sea_mask()
    data.set_hs()
    np.testing.assert_array_equal(
        data.sea_mask(), np.full(data.size(coord_group="grid"), True)
    )
    np.testing.assert_array_equal(
        data.land_mask(), np.full(data.size(coord_group="grid"), False)
    )
    data.set_sea_mask(data.hs()[0, :] > 0)
    np.testing.assert_array_equal(
        data.sea_mask(), np.full(data.size(coord_group="grid"), False)
    )
    np.testing.assert_array_equal(
        data.land_mask(), np.full(data.size(coord_group="grid"), True)
    )

    data.set_land_mask(data.hs()[0, :] <= 0)
    np.testing.assert_array_equal(
        data.sea_mask(), np.full(data.size(coord_group="grid"), False)
    )
    np.testing.assert_array_equal(
        data.land_mask(), np.full(data.size(coord_group="grid"), True)
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

    data = WaveHeight(x=(10, 20, 30), y=(30, 40, 50))
    data.set_sea_mask()
    data.set_hs()
    mask = data.sea_mask()

    lon, lat = data.sea_points(type="xy")
    np.testing.assert_array_almost_equal(lon, np.array([10, 20, 30]))
    np.testing.assert_array_almost_equal(lat, np.array([30, 40, 50]))

    lon, lat = data.land_points(type="xy")
    np.testing.assert_array_almost_equal(lon, np.array([]))
    np.testing.assert_array_almost_equal(lat, np.array([]))

    mask[0] = False
    data.set_sea_mask(mask)
    lon, lat = data.sea_points(coord="lon")
    assert lon is None  # No UTM set
    assert lat is None
    lon, lat = data.sea_points()
    np.testing.assert_array_almost_equal(lon, np.array([20, 30]))
    np.testing.assert_array_almost_equal(lat, np.array([40, 50]))

    lon, lat = data.land_points()
    np.testing.assert_array_almost_equal(lon, np.array([10]))
    np.testing.assert_array_almost_equal(lat, np.array([30]))


def test_add_mask_trigger():
    @add_mask(
        name="sea",
        default_value=1.0,
        opposite_name="land",
        triggered_by="hs",
        valid_range=(0, 3),
        range_inclusive=(False, True),
    )
    @add_datavar(name="hs", default_value=0.0)
    class WaveHeight(PointSkeleton):
        pass

    data = WaveHeight(lon=(10, 20), lat=(30, 40), z=(1, 2, 3))
    data.set_hs([0, 3])
    np.testing.assert_almost_equal(data.sea_mask(), np.array([False, True]))


def test_add_mask_trigger_inf():
    @add_mask(
        name="sea",
        default_value=1.0,
        opposite_name="land",
        triggered_by="hs",
        valid_range=(0, None),
        range_inclusive=False,
    )
    @add_datavar(name="hs", default_value=0.0)
    class WaveHeight(PointSkeleton):
        pass

    data = WaveHeight(lon=(10, 20), lat=(30, 40), z=(1, 2, 3))
    data.set_hs([0, 3])
    np.testing.assert_almost_equal(data.sea_mask(), np.array([False, True]))


def test_add_mask_trigger_two():
    @add_mask(
        name="point",
        default_value=0.0,
        triggered_by="hs",
        valid_range=(-999, -999),
    )
    @add_mask(
        name="sea",
        default_value=1.0,
        opposite_name="land",
        triggered_by="hs",
        valid_range=(0, None),
        range_inclusive=False,
    )
    @add_datavar(name="hs", default_value=0.0)
    class WaveHeight(PointSkeleton):
        pass

    data = WaveHeight(lon=(10, 20, 30), lat=(30, 40, 50), chunks="auto")
    data.dask.deactivate()
    data.set_hs([0, -999, 3])

    np.testing.assert_almost_equal(data.sea_mask(), np.array([False, False, True]))
    np.testing.assert_almost_equal(data.land_mask(), np.array([True, True, False]))
    np.testing.assert_almost_equal(data.point_mask(), np.array([False, True, False]))

    data.set_sea_mask([True, True, False])
    np.testing.assert_almost_equal(data.land_mask(), np.array([False, False, True]))
    np.testing.assert_almost_equal(data.sea_mask(), np.array([True, True, False]))
