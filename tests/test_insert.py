from geo_skeletons import PointSkeleton, GriddedSkeleton
from geo_skeletons.decorators import (
    add_coord,
    add_time,
    add_datavar,
    add_mask,
    activate_dask,
    add_magnitude,
)
import numpy as np
import pandas as pd


def test_insert_point():
    @add_datavar(name="hs", default_value=0)
    class WaveHeight(PointSkeleton):
        pass

    points = WaveHeight(x=(0, 1, 2), y=(5, 6, 7))
    points.set_hs()
    np.testing.assert_array_almost_equal(points.hs(), np.array([0, 0, 0]))
    points.ind_insert("hs", 1, inds=0)
    np.testing.assert_array_almost_equal(points.hs(), np.array([1, 0, 0]))
    points.insert("hs", 2, inds=1)
    np.testing.assert_array_almost_equal(points.hs(), np.array([1, 2, 0]))


def test_insert_point_dask():
    @activate_dask()
    @add_datavar(name="hs", default_value=0)
    class WaveHeight(PointSkeleton):
        pass

    points = WaveHeight(x=(0, 1, 2), y=(5, 6, 7))
    points.set_hs()
    np.testing.assert_array_almost_equal(points.hs(), np.array([0, 0, 0]))
    points.ind_insert("hs", 1, inds=0)
    np.testing.assert_array_almost_equal(points.hs(), np.array([1, 0, 0]))
    points.insert("hs", 2, inds=1)
    np.testing.assert_array_almost_equal(points.hs(), np.array([1, 2, 0]))


def test_insert_slice():
    @add_mask(name="sea", default_value=1.0, opposite_name="land")
    @add_datavar(name="hs", default_value=0)
    class WaveHeight(GriddedSkeleton):
        pass

    points = WaveHeight(lon=(0, 1), lat=(5, 6, 7))
    points.set_hs()
    np.testing.assert_array_almost_equal(
        points.hs(), np.array([[0, 0], [0, 0], [0, 0]])
    )
    points.ind_insert("hs", 1, lon=0)
    np.testing.assert_array_almost_equal(
        points.hs(), np.array([[1, 0], [1, 0], [1, 0]])
    )

    points.insert("hs", [2, 2], lat=6)
    np.testing.assert_array_almost_equal(
        points.hs(), np.array([[1, 0], [2, 2], [1, 0]])
    )

    points.insert("hs", 7, lat=7, lon=1)
    np.testing.assert_array_almost_equal(
        points.hs(), np.array([[1, 0], [2, 2], [1, 7]])
    )


def test_slice_magnitude():
    @add_magnitude(name="wind", x="u", y="v", direction="wdir", dir_type="from")
    @add_datavar("v", default_value=1)
    @add_datavar("u", default_value=1)
    class Wind(GriddedSkeleton):
        pass

    points = Wind(lon=(0, 1), lat=(5, 6, 7))
    points.set_u()
    points.set_v()
    np.testing.assert_array_almost_equal(points.u(), np.array([[1, 1], [1, 1], [1, 1]]))
    np.testing.assert_array_almost_equal(points.v(), np.array([[1, 1], [1, 1], [1, 1]]))
    np.testing.assert_array_almost_equal(
        points.wind(), np.array([[2**0.5, 2**0.5], [2**0.5, 2**0.5], [2**0.5, 2**0.5]])
    )
    np.testing.assert_array_almost_equal(
        points.wdir(), np.array([[225, 225], [225, 225], [225, 225]])
    )
    points.ind_insert("u", -1, lon=0)
    np.testing.assert_array_almost_equal(
        points.u(), np.array([[-1, 1], [-1, 1], [-1, 1]])
    )

    np.testing.assert_array_almost_equal(
        points.wind(), np.array([[2**0.5, 2**0.5], [2**0.5, 2**0.5], [2**0.5, 2**0.5]])
    )
    np.testing.assert_array_almost_equal(
        points.wdir(), np.array([[135, 225], [135, 225], [135, 225]])
    )

    points.insert("wind", 2, lon=0)
    np.testing.assert_array_almost_equal(
        points.u(),
        np.array([[-(2**0.5), 1], [-(2**0.5), 1], [-(2**0.5), 1]]),
    )
    np.testing.assert_array_almost_equal(
        points.v(),
        np.array([[(2**0.5), 1], [(2**0.5), 1], [(2**0.5), 1]]),
    )
    np.testing.assert_array_almost_equal(
        points.wind(), np.array([[2, 2**0.5], [2, 2**0.5], [2, 2**0.5]])
    )
    np.testing.assert_array_almost_equal(
        points.wdir(), np.array([[135, 225], [135, 225], [135, 225]])
    )

    points.insert("wdir", 270, lon=1)
    np.testing.assert_array_almost_equal(
        points.wind(), np.array([[2, 2**0.5], [2, 2**0.5], [2, 2**0.5]])
    )
    np.testing.assert_array_almost_equal(
        points.wdir(), np.array([[135, 270], [135, 270], [135, 270]])
    )
    np.testing.assert_array_almost_equal(
        points.u(),
        np.array([[-(2**0.5), 2**0.5], [-(2**0.5), 2**0.5], [-(2**0.5), 2**0.5]]),
    )
    np.testing.assert_array_almost_equal(
        points.v(),
        np.array([[(2**0.5), 0], [(2**0.5), 0], [(2**0.5), 0]]),
    )
