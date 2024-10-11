import xarray as xr
from geo_skeletons import GriddedSkeleton
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
    class WaveData(GriddedSkeleton):
        pass

    return WaveData


def test_trivial_remap(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time", "lon", "lat"],
        ds_lens=[24, 1, 1],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "lon": 1, "lat": 1},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time", "lon", "lat"]


def test_remap_wrong_name_for_trivial_dim(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time", "longitude", "latitude"],
        ds_lens=[24, 1, 1],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "lon": 1, "lat": 1},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time"]


def test_remap_wrong_name_for_one_nontrivial_dim(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time", "longitude", "latitude"],
        ds_lens=[24, 5, 1],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "lon": 5, "lat": 1},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time", "lon", "lat"]


def test_remap_not_trivial_inds_dim(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time"],
        ds_lens=[24],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "lon": 1, "lat": 1},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time"]


def test_two_non_trivial_dims(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time", "lon", "lat"],
        ds_lens=[24, 5, 6],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "lon": 5, "lat": 6},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time", "lon", "lat"]


def test_two_non_trivial_dims_one_wrong_name(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time", "lon2", "lat2"],
        ds_lens=[24, 5, 7],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "lon": 5, "lat": 7},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time", "lon", "lat"]


def test_two_non_trivial_dims_two_wrong_names(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time2", "lon2", "lat2"],
        ds_lens=[24, 5, 7],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "lon": 5, "lat": 7},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time", "lon", "lat"]


def test_two_non_trivial_dims_two_wrong_names_and_extra_trivial_dimesions(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["time2", "station", "lon2", "lat2", "ensemble"],
        ds_lens=[24, 1, 5, 7, 1],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "lon": 5, "lat": 7},
        conservative_expansion=False,
    )
    assert cg == "grid"
    assert coords == ["time", "lon", "lat"]


def test_spatial(wave_cls):
    coords, cg = _remap_ds_coords(
        ds_coords=["x", "y"],
        ds_lens=[5, 2],
        core=wave_cls.core,
        core_coords=CORE_COORDS,
        core_lens={"time": 24, "lon": 5, "lat": 2},
        conservative_expansion=False,
    )
    assert cg == "spatial"
    assert coords == ["lon", "lat"]
