from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_datavar, add_frequency, add_direction

import geo_parameters as gp

import pytest


def test_coord_meta():
    @add_direction()
    @add_frequency()
    @add_datavar(gp.wave.Hs("hs"))
    class WaveData(PointSkeleton):
        pass

    @add_direction(gp.wave.DirsTo)
    @add_frequency()
    @add_datavar(gp.wave.Hs("hs"))
    class WaveGrid(PointSkeleton):
        pass

    data = WaveData(lon=range(10), lat=range(10), freq=[1, 2, 3], dirs=[6, 7, 8])
    grid = WaveData(lon=range(10), lat=range(10), freq=[1, 2, 3], dirs=[6, 7, 8])

    data.set_hs(1)
    grid.set_hs(1)
    assert set(data.meta.meta_dict().keys()) == {
        "_global_",
        "lat",
        "lon",
        "inds",
        "freq",
        "dirs",
        "hs",
    }


def test_methods_respects_name():
    data = PointSkeleton(lon=[0, 1], lat=[0, 1], name="KeepThis")
    data2 = data.isel(inds=0)
    assert data2.name == "KeepThis"


def test_metadata_set_and_append():
    data = PointSkeleton.add_datavar(gp.wave.Hs)(
        lon=[0, 1], lat=[0, 1], name="KeepThis"
    )
    assert data.meta.get("hs").get("standard_name") == gp.wave.Hs.standard_name()
    data.set_hs(0)
    assert data.meta.get("hs").get("standard_name") == gp.wave.Hs.standard_name()
    assert data.ds().hs.standard_name == gp.wave.Hs.standard_name()
    assert data.meta.get().get("name") == "KeepThis"

    data.meta.append({"new_hs_info": "know this"}, "hs")
    data.meta.append({"new_global_info": "know this globally"})

    assert data.meta.get("hs").get("standard_name") == gp.wave.Hs.standard_name()
    assert data.meta.get().get("name") == "KeepThis"

    assert data.meta.get().get("new_global_info") == "know this globally"
    assert data.meta.get("hs").get("new_hs_info") == "know this"

    data.meta.set({"new_hs_info": "know this"}, "hs")
    data.meta.set({"new_global_info": "know this globally"})

    assert data.meta.get().get("new_global_info") == "know this globally"
    assert data.meta.get("hs").get("new_hs_info") == "know this"

    assert data.meta.get("hs").get("standard_name") is None
    assert data.meta.get().get("name") is None
