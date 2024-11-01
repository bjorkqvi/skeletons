from geo_skeletons.point_skeleton import PointSkeleton
from geo_skeletons.gridded_skeleton import GriddedSkeleton
from geo_skeletons.decorators import add_coord, add_datavar, add_time
import numpy as np
import geo_parameters as gp
import pytest
import pandas as pd
import xarray as xr


def test_time_in_lonlat_one_point_no_inds():
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 23:00", freq="1h")
    lon = np.arange(24)
    lat = np.arange(24) + 5
    hs = np.full(lon.shape, 3)

    ds = xr.Dataset(
        data_vars=dict(
            hs=("time", hs),
        ),
        coords=dict(lon=("time", lon), lat=("time", lat), time=time),
    )

    data = PointSkeleton.add_time().from_ds(ds, dynamic=True, verbose=True)
    assert data.core.coords(data.core.coord_group("hs")) == ["time", "inds"]
    assert set(data.ds().time.dims) == {"time"}
    assert set(data.ds().lon.dims) == {"inds"}
    assert set(data.ds().lat.dims) == {"inds"}


def test_time_in_lonlat_three_points_with_inds():
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 23:00", freq="1h")

    lon = np.stack((np.arange(24), np.arange(24), np.arange(24))).T
    lat = lon + 5
    hs = np.full(lon.shape, 3)
    inds = np.arange(3)

    ds = xr.Dataset(
        data_vars=dict(
            hs=(["time", "inds"], hs),
        ),
        coords=dict(
            lon=(["time", "inds"], lon),
            lat=(["time", "inds"], lat),
            time=time,
            inds=inds,
        ),
    )

    data = PointSkeleton.add_time().from_ds(ds, dynamic=True)
    assert data.core.coords(data.core.coord_group("hs")) == ["time", "inds"]
    assert set(data.ds().time.dims) == {"time"}
    assert set(data.ds().lon.dims) == {"inds"}
    assert set(data.ds().lat.dims) == {"inds"}


def test_time_in_lonlat_three_points_with_inds_wrong_dim_name():
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 23:00", freq="1h")

    lon = np.stack((np.arange(24), np.arange(24), np.arange(24))).T
    lat = lon + 5
    hs = np.full(lon.shape, 3)
    inds = np.arange(3)

    ds = xr.Dataset(
        data_vars=dict(
            hs=(["time", "station"], hs),
        ),
        coords=dict(
            lon=(["time", "station"], lon),
            lat=(["time", "station"], lat),
            time=time,
            station=inds,
        ),
    )

    data = PointSkeleton.add_time().from_ds(ds, dynamic=True)
    assert data.core.coords(data.core.coord_group("hs")) == ["time", "inds"]
    assert set(data.ds().time.dims) == {"time"}
    assert set(data.ds().lon.dims) == {"inds"}
    assert set(data.ds().lat.dims) == {"inds"}


def test_time_in_lonlat_three_points_with_inds_wrong_dim_name_wrong_order():
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 23:00", freq="1h")

    lon = np.stack((np.arange(24), np.arange(24), np.arange(24)))
    lat = lon + 5
    hs = np.full(lon.shape, 3)
    inds = np.arange(3)

    ds = xr.Dataset(
        data_vars=dict(
            hs=(["station", "time"], hs),
        ),
        coords=dict(
            lon=(["station", "time"], lon),
            lat=(["station", "time"], lat),
            time=time,
            station=inds,
        ),
    )

    data = PointSkeleton.add_time().from_ds(ds, dynamic=True, verbose=True)
    assert data.core.coords(data.core.coord_group("hs")) == ["time", "inds"]
    assert set(data.ds().time.dims) == {"time"}
    assert set(data.ds().lon.dims) == {"inds"}
    assert set(data.ds().lat.dims) == {"inds"}


def test_hs_only_time_one_point():
    time = pd.date_range("2020-01-01 00:00", "2020-01-01 23:00", freq="1h")
    lon = np.arange(1)
    lat = np.arange(1) + 5
    hs = np.full(time.shape, 3)
    inds = np.arange(1)
    ds = xr.Dataset(
        data_vars=dict(
            hs=("time", hs),
        ),
        coords=dict(lon=("inds", lon), lat=("inds", lat), time=time, inds=inds),
    )

    data = PointSkeleton.add_time().from_ds(ds, dynamic=True)
    assert data.core.coords(data.core.coord_group("hs")) == ["time", "inds"]
    assert set(data.ds().time.dims) == {"time"}
    assert set(data.ds().lon.dims) == {"inds"}
    assert set(data.ds().lat.dims) == {"inds"}


# def test_hs_only_time_three_point():
#     """We can't add inds to hs since inds is not trivial!

#     We can therefore only reorganize the data if we have a coordinate group with only time.

#     This can be done by adding time with grid_coord=False"""
#     time = pd.date_range("2020-01-01 00:00", "2020-01-01 23:00", freq="1h")
#     lon = np.arange(3)
#     lat = np.arange(3) + 5
#     hs = np.full(time.shape, 3)
#     inds = np.arange(3)
#     ds = xr.Dataset(
#         data_vars=dict(
#             hs=("time", hs),
#         ),
#         coords=dict(lon=("inds", lon), lat=("inds", lat), time=time, inds=inds),
#     )

#     data = PointSkeleton.add_time().from_ds(ds, dynamic=True)
#     assert data.core.coords(data.core.coord_group("hs")) == ["time", "inds"]
#     assert set(data.ds().time.dims) == {"time"}
#     assert set(data.ds().lon.dims) == {"inds"}
#     assert set(data.ds().lat.dims) == {"inds"}
# ds = xr.Dataset(
#     data_vars=dict(
#         hs=(["loc", "time"], hs),
#     ),
#     coords=dict(
#         lon=("loc", lon),
#         lat=("loc", lat),
#         instrument=instruments,
#         time=time,
#         reference_time=reference_time,
#     ),
#     attrs=dict(description="Weather related data."),
# )
