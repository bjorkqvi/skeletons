from geo_skeletons import PointSkeleton
from geo_skeletons.decorators import add_datavar, add_magnitude
import geo_parameters as gp
import pytest
from geo_skeletons.decoders import map_ds_to_gp
from geo_skeletons.decoders.ds_decoders import (
    _find_not_existing_vars,
    _find_xy_variables_present_in_ds,
    _find_magnitudes_and_directions_present_in_ds,
    _compile_list_of_addable_vars,
    _compile_list_of_addable_magnitudes_and_directions,
    _find_xy_variables_present_in_core,
    _find_mag_dir_datavars_present_in_core,
)


@pytest.fixture
def wind_xy():
    @add_datavar("dummy")
    @add_datavar("tp")
    @add_datavar(gp.wave.Hs("swh"))
    @add_datavar(gp.wind.YWind("uy"))
    @add_datavar(gp.wind.XWind("ux"))
    class WaveData(PointSkeleton):
        pass

    data = WaveData(lon=range(10), lat=range(10))
    data.set_uy(1)
    data.set_ux(10)
    data.set_swh(5)
    data.set_tp(15)
    data.set_dummy(6)
    return data


@pytest.fixture
def wind_magdir():
    @add_datavar("dummy")
    @add_datavar("tp")
    @add_datavar(gp.wave.Hs("swh"))
    @add_datavar(gp.wind.WindDir("ud"))
    @add_datavar(gp.wind.Wind("u"))
    class WaveData(PointSkeleton):
        pass

    data = WaveData(lon=range(10), lat=range(10))
    data.set_u(10)
    data.set_ud(100)
    data.set_swh(5)
    data.set_tp(15)
    data.set_dummy(6)

    return data


@pytest.fixture
def wind_magdirto():
    @add_datavar("dummy")
    @add_datavar("tp")
    @add_datavar(gp.wave.Hs("swh"))
    @add_datavar(gp.wind.WindDirTo("ud"))
    @add_datavar(gp.wind.Wind("u"))
    class WaveData(PointSkeleton):
        pass

    data = WaveData(lon=range(10), lat=range(10))
    data.set_u(10)
    data.set_ud(100)
    data.set_swh(5)
    data.set_tp(15)
    data.set_dummy(6)
    return data


def test_magdir(wind_magdir):
    ds_vars_to_gp, __ = map_ds_to_gp(
        wind_magdir.ds(), decode_cf=True, keep_ds_names=True, aliases={}
    )

    assert set(ds_vars_to_gp.keys()) == {"u", "ud", "swh", "tp", "dummy"}
    assert ds_vars_to_gp.get("u").is_same(gp.wind.Wind)
    assert ds_vars_to_gp.get("ud").is_same(gp.wind.WindDir)
    assert ds_vars_to_gp.get("swh").is_same(gp.wave.Hs)
    assert ds_vars_to_gp.get("u").name == "u"
    assert ds_vars_to_gp.get("ud").name == "ud"
    assert ds_vars_to_gp.get("swh").name == "swh"
    assert ds_vars_to_gp.get("tp").is_same(gp.wave.Tp)
    assert ds_vars_to_gp.get("tp").name == "tp"
    assert ds_vars_to_gp.get("dummy") == "dummy"

    addable_ds_vars = _find_not_existing_vars(
        ds_vars_to_gp, PointSkeleton.core, core_vars_to_ds_vars={}
    )

    assert set(addable_ds_vars) == {"u", "ud", "swh", "tp", "dummy"}

    xy_variables = _find_xy_variables_present_in_ds(addable_ds_vars, ds_vars_to_gp)
    assert xy_variables == []

    xy_variables_in_core = _find_xy_variables_present_in_core(PointSkeleton.core)
    mag_dir_datavars_in_core = _find_mag_dir_datavars_present_in_core(
        PointSkeleton.core
    )
    mag_dirs = _find_magnitudes_and_directions_present_in_ds(
        addable_ds_vars, ds_vars_to_gp
    )

    assert len(mag_dirs) == 1
    assert mag_dirs[0][0].is_same(gp.wind.Wind)
    assert mag_dirs[0][1].is_same(gp.wind.WindDir)

    addable_vars = _compile_list_of_addable_vars(
        xy_variables,
        xy_variables_in_core,
        mag_dirs,
        mag_dir_datavars_in_core,
        addable_ds_vars,
        ds_vars_to_gp,
    )

    assert len(addable_vars) == 5
    assert addable_vars[0].is_same(gp.wind.XWind)
    assert addable_vars[1].is_same(gp.wind.YWind)
    assert addable_vars[0].name == "x_wind"
    assert addable_vars[1].name == "y_wind"

    addable_magnitudes = _compile_list_of_addable_magnitudes_and_directions(
        addable_vars,
        xy_variables,
        xy_variables_in_core,
        mag_dirs,
        mag_dir_datavars_in_core,
    )

    assert len(addable_magnitudes) == 1
    assert addable_magnitudes[0].get("name").is_same(gp.wind.Wind)
    assert addable_magnitudes[0].get("direction").is_same(gp.wind.WindDir)
    assert addable_magnitudes[0].get("x") == "x_wind"
    assert addable_magnitudes[0].get("y") == "y_wind"


def test_xy(wind_xy):
    ds_vars_to_gp, __ = map_ds_to_gp(
        wind_xy.ds(), decode_cf=True, keep_ds_names=True, aliases={}
    )

    assert set(ds_vars_to_gp.keys()) == {"ux", "uy", "swh", "tp", "dummy"}
    assert ds_vars_to_gp.get("ux").is_same(gp.wind.XWind)
    assert ds_vars_to_gp.get("uy").is_same(gp.wind.YWind)
    assert ds_vars_to_gp.get("swh").is_same(gp.wave.Hs)
    assert ds_vars_to_gp.get("uy").name == "uy"
    assert ds_vars_to_gp.get("ux").name == "ux"
    assert ds_vars_to_gp.get("swh").name == "swh"
    assert ds_vars_to_gp.get("tp").is_same(gp.wave.Tp)
    assert ds_vars_to_gp.get("tp").name == "tp"
    assert ds_vars_to_gp.get("dummy") == "dummy"

    addable_ds_vars = _find_not_existing_vars(
        ds_vars_to_gp, PointSkeleton.core, core_vars_to_ds_vars={}
    )

    assert set(addable_ds_vars) == {"ux", "uy", "swh", "tp", "dummy"}

    xy_variables = _find_xy_variables_present_in_ds(addable_ds_vars, ds_vars_to_gp)
    assert len(xy_variables) == 1
    assert xy_variables[0][0].is_same(gp.wind.XWind)
    assert xy_variables[0][1].is_same(gp.wind.YWind)

    xy_variables_in_core = _find_xy_variables_present_in_core(PointSkeleton.core)
    mag_dir_datavars_in_core = _find_mag_dir_datavars_present_in_core(
        PointSkeleton.core
    )

    mag_dirs = _find_magnitudes_and_directions_present_in_ds(
        addable_ds_vars, ds_vars_to_gp
    )

    assert mag_dirs == []

    addable_vars = _compile_list_of_addable_vars(
        xy_variables,
        xy_variables_in_core,
        mag_dirs,
        mag_dir_datavars_in_core,
        addable_ds_vars,
        ds_vars_to_gp,
    )

    assert len(addable_vars) == 5
    assert addable_vars[0].is_same(gp.wind.XWind)
    assert addable_vars[1].is_same(gp.wind.YWind)
    assert addable_vars[0].name == "ux"
    assert addable_vars[1].name == "uy"

    addable_magnitudes = _compile_list_of_addable_magnitudes_and_directions(
        addable_vars,
        xy_variables,
        xy_variables_in_core,
        mag_dirs,
        mag_dir_datavars_in_core,
    )

    assert len(addable_magnitudes) == 1
    assert addable_magnitudes[0].get("name").is_same(gp.wind.Wind)
    assert addable_magnitudes[0].get("direction").is_same(gp.wind.WindDir)
    assert addable_magnitudes[0].get("x") == "ux"
    assert addable_magnitudes[0].get("y") == "uy"


def test_magdirto(wind_magdirto):
    ds_vars_to_gp, __ = map_ds_to_gp(
        wind_magdirto.ds(), decode_cf=True, keep_ds_names=True, aliases={}
    )
    assert set(ds_vars_to_gp.keys()) == {"u", "ud", "swh", "tp", "dummy"}
    assert ds_vars_to_gp.get("u").is_same(gp.wind.Wind)
    assert ds_vars_to_gp.get("ud").is_same(gp.wind.WindDirTo)
    assert ds_vars_to_gp.get("swh").is_same(gp.wave.Hs)
    assert ds_vars_to_gp.get("u").name == "u"
    assert ds_vars_to_gp.get("ud").name == "ud"
    assert ds_vars_to_gp.get("swh").name == "swh"
    assert ds_vars_to_gp.get("tp").is_same(gp.wave.Tp)
    assert ds_vars_to_gp.get("tp").name == "tp"
    assert ds_vars_to_gp.get("dummy") == "dummy"

    addable_ds_vars = _find_not_existing_vars(
        ds_vars_to_gp, PointSkeleton.core, core_vars_to_ds_vars={}
    )

    assert set(addable_ds_vars) == {"u", "ud", "swh", "tp", "dummy"}

    xy_variables = _find_xy_variables_present_in_ds(addable_ds_vars, ds_vars_to_gp)
    assert xy_variables == []
    xy_variables_in_core = _find_xy_variables_present_in_core(PointSkeleton.core)
    mag_dir_datavars_in_core = _find_mag_dir_datavars_present_in_core(
        PointSkeleton.core
    )
    mag_dirs = _find_magnitudes_and_directions_present_in_ds(
        addable_ds_vars, ds_vars_to_gp
    )

    assert len(mag_dirs) == 1
    assert mag_dirs[0][0].is_same(gp.wind.Wind)
    assert mag_dirs[0][1].is_same(gp.wind.WindDir)

    addable_vars = _compile_list_of_addable_vars(
        xy_variables,
        xy_variables_in_core,
        mag_dirs,
        mag_dir_datavars_in_core,
        addable_ds_vars,
        ds_vars_to_gp,
    )

    assert len(addable_vars) == 5
    assert addable_vars[0].is_same(gp.wind.XWind)
    assert addable_vars[1].is_same(gp.wind.YWind)
    assert addable_vars[0].name == "x_wind"
    assert addable_vars[1].name == "y_wind"

    addable_magnitudes = _compile_list_of_addable_magnitudes_and_directions(
        addable_vars,
        xy_variables,
        xy_variables_in_core,
        mag_dirs,
        mag_dir_datavars_in_core,
    )

    assert len(addable_magnitudes) == 1
    assert addable_magnitudes[0].get("name").is_same(gp.wind.Wind)
    assert addable_magnitudes[0].get("direction").is_same(gp.wind.WindDir)
    assert addable_magnitudes[0].get("x") == "x_wind"
    assert addable_magnitudes[0].get("y") == "y_wind"
