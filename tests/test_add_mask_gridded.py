from geo_skeletons.gridded_skeleton import GriddedSkeleton
from geo_skeletons.decorators import add_coord, add_time, add_datavar, add_mask

import numpy as np
import pandas as pd


def test_add_mask_one_point():
    @add_mask(name="sea", default_value=1.0, opposite_name="land")
    @add_datavar(name="hs", default_value=0)
    class WaveHeight(GriddedSkeleton):
        pass

    data = WaveHeight(lon=(10), lat=(30))

    data.set_spacing(nx=10, ny=10)

    np.testing.assert_array_equal(data.sea_mask(empty=True), np.full(data.size(), True))
    assert data.sea_mask(empty=True).shape == data.size()

    np.testing.assert_array_equal(
        data.land_mask(empty=True), np.full(data.size(), False)
    )
    data.set_sea_mask(data.hs(empty=True) > 0)
    np.testing.assert_array_equal(data.sea_mask(), np.full(data.size(), False))
    np.testing.assert_array_equal(data.land_mask(), np.full(data.size(), True))
    assert data.core.masks()[0] == "sea_mask"


def test_add_mask():
    @add_mask(name="sea", default_value=1.0, opposite_name="land")
    @add_datavar(name="hs", default_value=0)
    class WaveHeight(GriddedSkeleton):
        pass

    data = WaveHeight(lon=(10, 20), lat=(30, 40))
    data.set_spacing(nx=10, ny=10)

    np.testing.assert_array_equal(data.sea_mask(empty=True), np.full(data.size(), True))

    np.testing.assert_array_equal(
        data.land_mask(empty=True), np.full(data.size(), False)
    )
    data.set_sea_mask(data.hs(empty=True) > 0)
    np.testing.assert_array_equal(data.sea_mask(), np.full(data.size(), False))
    np.testing.assert_array_equal(data.land_mask(), np.full(data.size(), True))
    assert data.core.masks()[0] == "sea_mask"


def test_add_coord_and_mask():
    @add_mask(name="sea", default_value=1.0, opposite_name="land")
    @add_datavar(name="hs", default_value=0.0)
    @add_coord(name="z", grid_coord=True)
    class WaveHeight(GriddedSkeleton):
        pass

    data = WaveHeight(lon=(10, 20), lat=(30, 40), z=(1, 2, 3))
    data.set_spacing(nx=10, ny=10)
    data.set_z_spacing(nx=4)
    np.testing.assert_array_equal(data.sea_mask(empty=True), np.full(data.size(), True))
    np.testing.assert_array_equal(
        data.land_mask(empty=True), np.full(data.size(), False)
    )
    data.set_sea_mask(data.hs(empty=True) > 0)
    np.testing.assert_array_equal(data.sea_mask(), np.full(data.size(), False))
    np.testing.assert_array_equal(data.land_mask(), np.full(data.size(), True))

    assert data.core.masks()[0] == "sea_mask"


def test_add_gridpoint_coord_and_mask():
    @add_mask(name="sea", default_value=1.0, opposite_name="land", coord_group="grid")
    @add_datavar(name="hs", default_value=0.0)
    @add_time(grid_coord=False)
    @add_coord(name="z", grid_coord=True)
    class WaveHeight(GriddedSkeleton):
        pass

    times = pd.date_range("2018-01-01 00:00", "2018-02-01 00:00", freq="1h")
    data = WaveHeight(lon=(10, 20), lat=(30, 40), z=(1, 2, 3), time=times)
    data.set_spacing(nx=10, ny=10)
    data.set_z_spacing(nx=4)
    data.set_sea_mask()

    np.testing.assert_array_equal(
        data.sea_mask(), np.full(data.size(coord_group="grid"), True)
    )
    np.testing.assert_array_equal(
        data.land_mask(), np.full(data.size(coord_group="grid"), False)
    )
    data.set_sea_mask(data.hs(empty=True)[0, :] > 0)
    np.testing.assert_array_equal(
        data.sea_mask(), np.full(data.size(coord_group="grid"), False)
    )
    np.testing.assert_array_equal(
        data.land_mask(), np.full(data.size(coord_group="grid"), True)
    )


def test_get_points():
    @add_mask(name="sea", default_value=1.0, opposite_name="land")
    @add_datavar(name="hs", default_value=0)
    class WaveHeight(GriddedSkeleton):
        pass

    data = WaveHeight(lon=(10, 30), lat=(30, 50))
    data.set_spacing(nx=3, ny=3)
    mask = data.sea_mask(empty=True)

    lon, lat = data.sea_points()
    lon_all, lat_all = data.lonlat()
    np.testing.assert_array_almost_equal(lon, lon_all)
    np.testing.assert_array_almost_equal(lat, lat_all)

    lon, lat = data.land_points()
    np.testing.assert_array_almost_equal(lon, np.array([]))
    np.testing.assert_array_almost_equal(lat, np.array([]))

    mask[0, :] = False
    data.set_sea_mask(mask)
    lon, lat = data.sea_points()
    np.testing.assert_array_almost_equal(lon, lon_all[3:])
    np.testing.assert_array_almost_equal(lat, lat_all[3:])

    lon, lat = data.land_points()
    np.testing.assert_array_almost_equal(lon, lon_all[:3])
    np.testing.assert_array_almost_equal(lat, lat_all[:3])
