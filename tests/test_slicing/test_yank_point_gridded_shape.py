from geo_skeletons import PointSkeleton, GriddedSkeleton
import numpy as np
import pytest


def test_gridded_shape():
    grid = GriddedSkeleton(lon=(10, 11), lat=(0, 1))
    grid.set_spacing(nx=10, ny=5)

    ind_dict_gridded = grid.yank_point(lon=10.09, lat=0.51)
    lon, lat = grid.lonlat()
    points = PointSkeleton(lon=lon, lat=lat)
    ind_dict = points.yank_point(lon=10.09, lat=0.51, gridded_shape=grid.size())

    assert ind_dict_gridded["inds_y"][0] == ind_dict["inds_y"][0]
    assert ind_dict_gridded["inds_x"][0] == ind_dict["inds_x"][0]


def test_gridded_shape_wrong_shape():
    grid = GriddedSkeleton(lon=(10, 11), lat=(0, 1))
    grid.set_spacing(nx=10, ny=5)

    ind_dict_gridded = grid.yank_point(lon=10.09, lat=0.51)
    lon, lat = grid.lonlat()
    points = PointSkeleton(lon=lon, lat=lat)
    with pytest.raises(ValueError):
        ind_dict = points.yank_point(lon=10.09, lat=0.51, gridded_shape=(1, 10))


def test_gridded_shape_two_points():
    grid = GriddedSkeleton(lon=(10, 11), lat=(0, 1))
    grid.set_spacing(nx=10, ny=5)

    ind_dict_gridded = grid.yank_point(lon=10.09, lat=0.51, npoints=2)
    lon, lat = grid.lonlat()
    points = PointSkeleton(lon=lon, lat=lat)
    ind_dict = points.yank_point(
        lon=10.09, lat=0.51, gridded_shape=grid.size(), npoints=2
    )

    assert ind_dict_gridded["inds_y"][0] == ind_dict["inds_y"][0]
    assert ind_dict_gridded["inds_x"][0] == ind_dict["inds_x"][0]
    assert ind_dict_gridded["inds_y"][1] == ind_dict["inds_y"][1]
    assert ind_dict_gridded["inds_x"][1] == ind_dict["inds_x"][1]
