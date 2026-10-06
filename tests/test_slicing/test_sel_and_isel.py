from geo_skeletons import PointSkeleton, GriddedSkeleton
import numpy as np
from geo_skeletons.decorators import add_datavar

import geo_parameters as gp
import pytest
from geo_skeletons.errors import SkeletonError

def test_point():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))

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
    @add_datavar(name="dummyvar")
    class Dummy(PointSkeleton):
        pass

    points = Dummy(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))

    points.set_dummyvar(5)
    assert points.core.data_vars() == ["dummyvar"]

    points2 = points.isel(inds=1)
    assert points2.core.data_vars() == ["dummyvar"]


def test_point_dynamic_datavar():
    class Dummy(PointSkeleton):
        pass

    points = Dummy.add_datavar("dummyvar")(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))
    points.set_dummyvar(5)
    assert points.core.data_vars() == ["dummyvar"]

    points2 = points.isel(inds=1)
    assert points2.core.data_vars() == ["dummyvar"]


def test_point_dynamic_datavar_geoparam():
    class Dummy(PointSkeleton):
        pass

    points = Dummy.add_datavar(gp.wave.Hs("hsig"))(
        lon=(1, 2, 3, 4), lat=(10, 20, 30, 40)
    )

    points.set_hsig(5)
    assert points.core.data_vars() == ["hsig"]

    points2 = points.isel(inds=1)
    assert points2.core.data_vars() == ["hsig"]
    assert points2.meta.get("hsig") == points.meta.get("hsig")


def test_point_dynamic_datavars_from_ds():

    class Dummy(PointSkeleton):
        pass

    points = Dummy.add_datavar(gp.wave.Hs("hsig"))(
        lon=(1, 2, 3, 4), lat=(10, 20, 30, 40)
    )
    points.set_hsig(5)
    assert points.core.data_vars() == ["hsig"]

    ds = points.ds()
    points2 = PointSkeleton.from_ds(ds, dynamic=True, keep_ds_names=True)

    assert points2.core.data_vars() == ["hsig"]
    assert points2.meta.get("hsig") == points.meta.get("hsig")

    points3 = PointSkeleton.from_ds(ds, dynamic=True)

    assert points3.core.data_vars() == ["hs"]
    assert points3.meta.get("hs") == points.meta.get("hsig")


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


def test_slice_lon_lat_in_pointskeleton():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))
    points2 = points.sel(lon=slice(2, 3))
    np.testing.assert_array_almost_equal(points2.lon(), (2, 3))
    np.testing.assert_array_almost_equal(points2.lat(), (20, 30))
    points3 = points.sel(lon=slice(1, 3), lat=slice(20, 40))
    np.testing.assert_array_almost_equal(points3.lon(), (2, 3))
    np.testing.assert_array_almost_equal(points3.lat(), (20, 30))


def test_slice_x_y_in_pointskeleton():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))
    x0, x1 = points.x()[1:3]
    #breakpoint()
    points2 = points.sel(x=slice(x0, x1))
    np.testing.assert_array_almost_equal(points2.lon(), points.lon()[1:3])
    np.testing.assert_array_almost_equal(points2.lat(), points.lat()[1:3])
    x0, x1 = points.x()[0], points.x()[2]
    y0, y1 = points.y()[1], points.y()[3]

    points2 = points.sel(x=slice(x0, x1), y=slice(y0, y1))
    np.testing.assert_array_almost_equal(points2.lon(), points.lon()[1:3])
    np.testing.assert_array_almost_equal(points2.lat(), points.lat()[1:3])


def test_yank_slice_lon_lat_in_pointskeleton():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))
    points2 = points.sel(lon=2.1, lat=20.05)
    np.testing.assert_array_almost_equal(points2.lon(), 2)
    np.testing.assert_array_almost_equal(points2.lat(), 20)


def test_yank_slice_x_y_in_pointskeleton():
    points = PointSkeleton(lon=(1, 2, 3, 4), lat=(10, 20, 30, 40))
    x, y = points.x()[1] + 1, points.y()[1] - 1

    points2 = points.sel(x=x, y=y)

    np.testing.assert_array_almost_equal(points2.lon(), 2)
    np.testing.assert_array_almost_equal(points2.lat(), 20)

def test_sel_one_lonlat_point_from_xy_gridded():
    grid = GriddedSkeleton(lon=(5,6,7), lat=(50,51,52))
    utm_grid = grid.proj_grid(crs=(33,'W'))
    point = utm_grid.sel(lon=6, lat=51)
    # There is a rotation etc so won't match exactly here
    np.testing.assert_almost_equal(6, point.lon(), decimal=1)
    np.testing.assert_almost_equal(51, point.lat(), decimal=1)
    

def test_sel_one_xy_point_from_lonlat_gridded():
    grid = GriddedSkeleton(lon=(5,6,7), lat=(50,51,52))
    x0, y0 = grid.ravel().x()[2], grid.ravel().y()[2]
    point = grid.sel(x=x0, y=y0)
    np.testing.assert_almost_equal(grid.ravel().lon()[2], point.lon())
    np.testing.assert_almost_equal(grid.ravel().lat()[2], point.lat())
    

def test_sel_one_lonlat_point_from_xy_gridded_with_third_coordinate():
    grid = GriddedSkeleton.add_coord('depth')(lon=(5,6,7), lat=(50,51,52), depth=(10,20, 30,40))
    utm_grid = grid.proj_grid(crs=(33,'W'))
    point = utm_grid.sel(lon=6, lat=51)
    # There is a rotation etc so won't match exactly here
    np.testing.assert_almost_equal(6, point.lon(), decimal=1)
    np.testing.assert_almost_equal(51, point.lat(), decimal=1)
    np.testing.assert_almost_equal((10,20, 30,40), point.depth())

    point = utm_grid.sel(lon=6, lat=51, depth=slice(19,31))
    # There is a rotation etc so won't match exactly here
    np.testing.assert_almost_equal(6, point.lon(), decimal=1)
    np.testing.assert_almost_equal(51, point.lat(), decimal=1)
    np.testing.assert_almost_equal((20, 30), point.depth())

def test_sel_one_xy_point_from_lonlat_gridded_with_third_coordinate():
    grid = GriddedSkeleton.add_coord('depth')(lon=(5,6,7), lat=(50,51,52), depth=(10,20, 30,40))
    x0, y0 = grid.ravel().x()[2], grid.ravel().y()[2]
    point = grid.sel(x=x0, y=y0)
    np.testing.assert_almost_equal(grid.ravel().lon()[2], point.lon())
    np.testing.assert_almost_equal(grid.ravel().lat()[2], point.lat())
    np.testing.assert_almost_equal((10,20, 30,40), point.depth())

    point = grid.sel(x=x0, y=y0, depth=slice(20,30))
    np.testing.assert_almost_equal(grid.ravel().lon()[2], point.lon())
    np.testing.assert_almost_equal(grid.ravel().lat()[2], point.lat())
    np.testing.assert_almost_equal((20, 30), point.depth())

def test_sel_two_lonlat_points_from_xy_gridded():
    grid = GriddedSkeleton(lon=(5,6,7), lat=(50,51,52))
    utm_grid = grid.proj_grid(crs=(33,'W'))
    point = utm_grid.sel(lon=(6, 7), lat=(51, 52))
    assert point.size() == (2,2)

    point = utm_grid.sel(lon=(6, 6), lat=(51, 52))
    assert point.size() == (2,1)
    
    # There is a rotation etc so won't match exactly here
    np.testing.assert_almost_equal(6, point.ravel().lon()[0], decimal=1)
    np.testing.assert_almost_equal(51, point.ravel().lat()[0], decimal=1)

    np.testing.assert_almost_equal(6, point.ravel().lon()[1], decimal=0)
    np.testing.assert_almost_equal(52, point.ravel().lat()[1], decimal=1)


def test_sel_two_xy_points_from_lonlat_gridded():
    grid = GriddedSkeleton(lon=(5,6,7), lat=(50,51,52))

    x0, y0 = grid.ravel().x()[2], grid.ravel().y()[2:4]
    with pytest.raises(ValueError): # 1 x and 2 y coordinates wont work
        point = grid.sel(x=x0, y=y0)

    x0, y0 = grid.ravel().x()[2:4], grid.ravel().y()[2:4]
    point = grid.sel(x=x0, y=y0)
    
    assert point.size() == (2,2)


    x0, y0 = grid.ravel().x()[2], grid.ravel().y()[2:4]
    point = grid.sel(x=(x0, x0), y=y0)
    assert point.size() == (2,1)

    np.testing.assert_almost_equal(grid.ravel().lon()[2], point.lon())
    np.testing.assert_almost_equal(grid.ravel().lat()[2:4], point.lat())


def test_sel_lonlat_slice_from_xy_gridded():
    grid = GriddedSkeleton(lon=(5,6,7), lat=(50,51,52))
    utm_grid = grid.proj_grid(crs=(33,'W'))

    point = utm_grid.sel(lon=slice(5.5,6.6))
    assert point.size() == (3,1)

    # There is a rotation etc so won't match exactly here
    np.testing.assert_almost_equal([6,6,6], point.ravel().lon(), decimal=0)
    np.testing.assert_almost_equal([50,51,52], point.ravel().lat(), decimal=1)


    point = utm_grid.sel(lon=slice(5.5,6.6), lat=51)
    assert point.size() == (1,1)

    # There is a rotation etc so won't match exactly here
    np.testing.assert_almost_equal([6], point.ravel().lon(), decimal=0)
    np.testing.assert_almost_equal([51], point.ravel().lat(), decimal=1)


    point = utm_grid.sel(lon=slice(5.5,6.6), lat=slice(50.9, 51.1))
    assert point.size() == (1,1)

    # There is a rotation etc so won't match exactly here
    np.testing.assert_almost_equal([6], point.ravel().lon(), decimal=0)
    np.testing.assert_almost_equal([51], point.ravel().lat(), decimal=1)

    with pytest.raises(SkeletonError):
        point = utm_grid.sel(lat=51)
    point = utm_grid.sel(lat=slice(50.9, 51.1))
    assert point.size() == (1,3)

    # There is a rotation etc so won't match exactly here
    np.testing.assert_almost_equal([5,6,7], point.ravel().lon(), decimal=0)
    np.testing.assert_almost_equal([51,51,51], point.ravel().lat(), decimal=0)


def test_sel_xy_slice_from_lonlat_gridded():
    grid = GriddedSkeleton(lon=(5,6,7), lat=(50,51,52))
    x0 = grid.ravel().x()[4]
    y0 = grid.ravel().y()[4]


    point = grid.sel(x=slice(x0-20000, x0+20000))
    assert point.size() == (3,1)

    # There is a rotation etc so won't match exactly here

    np.testing.assert_almost_equal([x0/10_000,x0/10_000,x0/10_000], point.ravel().x()/10_000, decimal=0)
    np.testing.assert_almost_equal(grid.ygrid()[:,1]/10_000, point.ravel().y()/10_000, decimal=0)


    point = grid.sel(x=slice(x0-20000, x0+20000), y=y0)
    assert point.size() == (1,1)

    # There is a rotation etc so won't match exactly here
    np.testing.assert_almost_equal([x0/10_000], point.ravel().x()/10_000, decimal=0)
    np.testing.assert_almost_equal([y0/10_000], point.ravel().y()/10_000, decimal=0)


    point = grid.sel(x=slice(x0-20000, x0+20000), y=slice(y0-20000, y0+20000))
    assert point.size() == (1,1)

    np.testing.assert_almost_equal([x0/10_000], point.ravel().x()/10_000, decimal=0)
    np.testing.assert_almost_equal([y0/10_000], point.ravel().y()/10_000, decimal=0)

    with pytest.raises(SkeletonError):
        point = grid.sel(y=y0+100)
    point = grid.sel(y=slice(y0-20000, y0+20000))
    assert point.size() == (1,3)

    # # There is a rotation etc so won't match exactly here
    np.testing.assert_almost_equal([y0/10_000,y0/10_000,y0/10_000], point.ravel().y()/10_000, decimal=0)
    np.testing.assert_almost_equal(grid.xgrid()[1,:]/10_000, point.ravel().x()/10_000, decimal=0)

  