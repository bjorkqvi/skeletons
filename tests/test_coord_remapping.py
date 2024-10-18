import xarray as xr
from geo_skeletons import PointSkeleton
from geo_skeletons.decorators import add_time, add_datavar
import geo_parameters as gp
from geo_skeletons.decoders.coord_remapping import _remap_ds_coords
import xarray as xr
import pytest
import pandas as pd
import numpy as np

CORE_COORDS = {"time": "time", "lon": "lon", "lat": "lat"}


@pytest.fixture
def wave_cls():
    @add_datavar(gp.wave.Hs)
    @add_time()
    class WaveData(PointSkeleton):
        pass

    return WaveData


@pytest.fixture
def wave_cls_gridpoint():
    @add_datavar(gp.wave.Hs)
    @add_time(grid_coord=False)
    class WaveData(PointSkeleton):
        pass

    return WaveData


def test_trivial_remap(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time", "inds"],
        ds_lens=[24, 1],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "inds": 1},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time", "inds"]


def test_remap_wrong_name_for_trivial_dim(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time", "station"],
        ds_lens=[24, 1],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "inds": 1},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time", "inds"]


def test_remap_not_trivial_inds_dim(wave_cls, wave_cls_gridpoint):
    coords, cg = _remap_ds_coords(
        ds_coords=["time"],
        ds_lens=[24],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "inds": 1},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time"]

    coords, cg = _remap_ds_coords(
        ds_coords=["time"],
        ds_lens=[24],
        core=wave_cls_gridpoint.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "inds": 1},
        conservative_expansion=False,
    )
    assert cg == "all"
    assert coords == ["time"]

    coords, cg = _remap_ds_coords(
        ds_coords=["time"],
        ds_lens=[24],
        core=wave_cls_gridpoint.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "inds": 1},
        conservative_expansion=True,
    )
    assert cg == "gridpoint"
    assert coords == ["time"]


def test_two_non_trivial_dims(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time", "inds"],
        ds_lens=[24, 5],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "inds": 5},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time", "inds"]


def test_two_non_trivial_dims_one_wrong_name(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time", "inds2"],
        ds_lens=[24, 5],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "inds": 5},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time", "inds"]


def test_two_non_trivial_dims_two_wrong_names(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time2", "inds2"],
        ds_lens=[24, 5],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "inds": 5},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time", "inds"]


def test_two_non_trivial_dims_two_wrong_names_and_extra_trivial_dimesions(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time2", "station", "inds2", "ensemble"],
        ds_lens=[24, 1, 5, 1],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "inds": 5},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time", "inds"]


def test_xy_instead_of_inds(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time", "x", "y"],
        ds_lens=[24, 5, 1],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "inds": 5},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time", "inds"]


def test_spatial(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["x", "y"],
        ds_lens=[5, 1],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "inds": 5},
        conservative_expansion=False,
    )
    assert cg == "spatial"
    assert coords == ["inds"]
