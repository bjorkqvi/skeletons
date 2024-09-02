from geo_skeletons import PointSkeleton, GriddedSkeleton
import numpy as np
from geo_skeletons.decorators import add_datavar, dynamic

import geo_parameters as gp


def test_point():
    assert PointSkeleton.core.static
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))

    assert PointSkeleton.core.static
    assert points.core.static
    np.testing.assert_array_almost_equal(
        points.lon(), points.sel(inds=slice(0, 10)).lon()
    )

    np.testing.assert_array_almost_equal([3, 4], points.sel(inds=slice(2, 4)).lon())
    np.testing.assert_array_almost_equal([1, 4], points.sel(inds=[0, 3]).lon())

    np.testing.assert_array_almost_equal([10, 40], points.sel(inds=[0, 3]).lat())

    np.testing.assert_array_almost_equal(
        points.lon(), points.isel(inds=slice(0, 10)).lon()
    )

    np.testing.assert_array_almost_equal([3, 4], points.isel(inds=slice(2, 4)).lon())
    np.testing.assert_array_almost_equal([1, 4], points.isel(inds=[0, 3]).lon())

    np.testing.assert_array_almost_equal([10, 40], points.isel(inds=[0, 3]).lat())


def test_point_old_datavar():
    assert PointSkeleton.core.static

    @add_datavar(name="dummyvar")
    class Dummy(PointSkeleton):
        pass

    assert Dummy.core.static

    points = Dummy(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))

    assert points.core.static

    points.set_dummyvar(5)
    assert points.core.data_vars() == ["dummyvar"]

    points2 = points.isel(inds=1)
    assert points2.core.data_vars() == ["dummyvar"]


def test_point_dynamic_datavar():
    assert PointSkeleton.core.static

    @dynamic
    class Dummy(PointSkeleton):
        pass

    assert PointSkeleton.core.static
    assert not Dummy.core.static

    points = Dummy(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))
    points.add_datavar("dummyvar")
    points.set_dummyvar(5)
    assert points.core.data_vars() == ["dummyvar"]

    points2 = points.isel(inds=1)
    assert points2.core.data_vars() == ["dummyvar"]


def test_point_dynamic_datavar_geoparam():
    assert PointSkeleton.core.static

    @dynamic
    class Dummy(PointSkeleton):
        pass

    assert PointSkeleton.core.static
    assert not Dummy.core.static

    points = Dummy(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))
    points.add_datavar(gp.wave.Hs("hsig"))
    points.set_hsig(5)
    assert points.core.data_vars() == ["hsig"]

    points2 = points.isel(inds=1)
    assert points2.core.data_vars() == ["hsig"]
    assert points2.meta.get("hsig") == points.meta.get("hsig")


def test_point_dynamic_datavars_from_ds():
    assert PointSkeleton.core.static

    @dynamic
    class Dummy(PointSkeleton):
        pass

    assert PointSkeleton.core.static
    assert not Dummy.core.static

    points = Dummy(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))
    points.add_datavar(gp.wave.Hs("hsig"))
    points.set_hsig(5)
    assert points.core.data_vars() == ["hsig"]

    ds = points.ds()

    points2 = PointSkeleton.from_ds(ds, data_vars=ds.data_vars)
    assert PointSkeleton.core.static
    assert points2.core.static

    assert points2.core.data_vars() == ["hsig"]
    assert points2.meta.get("hsig") == points.meta.get("hsig")


def test_gridded():
    points = GriddedSkeleton(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))
    np.testing.assert_array_almost_equal(
        points.lon(), points.sel(lon=slice(0, 10)).lon()
    )
    np.testing.assert_array_almost_equal(points.lon(), points.sel(lat=10).lon())
    np.testing.assert_array_almost_equal([1, 2, 3], points.sel(lon=slice(1, 3)).lon())

    np.testing.assert_array_almost_equal(
        [1, 2, 3], points.sel(lon=slice(1, 3), lat=10).lon()
    )

    np.testing.assert_array_almost_equal([1, 2, 4], points.sel(lon=[1, 2, 4]).lon())

    np.testing.assert_array_almost_equal([1, 2, 4], points.isel(lon=[0, 1, 3]).lon())
    np.testing.assert_array_almost_equal(points.lat(), points.isel(lon=[0, 1, 3]).lat())

    np.testing.assert_array_almost_equal([1, 2, 4], points.isel(lon=[0, 0, 1, 3]).lon())
