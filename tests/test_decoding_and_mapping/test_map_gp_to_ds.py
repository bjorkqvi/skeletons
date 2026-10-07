from geo_skeletons import PointSkeleton, GriddedSkeleton
from geo_skeletons.decorators import (
    add_datavar,
    add_frequency,
    add_direction,
    add_coord,
)

import geo_parameters as gp

from geo_skeletons.decoders.core_decoders import (
    _map_geo_parameter_to_ds_variable,
    _map_similar_geo_parameter_to_ds_variable,
    _map_geo_parameter_to_components_in_ds,
)

import pytest


@pytest.fixture
def wave_no_std():
    @add_datavar("dirp")
    @add_datavar("tp")
    @add_datavar("hs")
    class WaveData(PointSkeleton):
        pass

    data = WaveData(x=range(10), y=range(10))
    data.set_hs(1)
    data.set_tp(10)
    data.set_dirp(90)

    return data


@pytest.fixture
def wave_std():
    @add_datavar(gp.wave.Dirp)
    @add_datavar(gp.wave.Tp)
    @add_datavar(gp.wave.Hs)
    class WaveData(PointSkeleton):
        pass

    data = WaveData(lon=range(10), lat=range(10))
    data.set_hs(1)
    data.set_tp(10)
    data.set_dirp(90)

    return data


@pytest.fixture
def wave2_no_std():
    @add_datavar("dirp2")
    @add_datavar("tp2")
    @add_datavar("hs2")
    class WaveData(GriddedSkeleton):
        pass

    data = WaveData(x=range(10), y=range(10))
    data.set_hs2(1)
    data.set_tp2(10)
    data.set_dirp2(90)

    return data


@pytest.fixture
def wave2_std():
    @add_datavar(gp.wave.Dirp("dirp2"))
    @add_datavar(gp.wave.Tp("tp2"))
    @add_datavar(gp.wave.Hs("hs2"))
    class WaveData(GriddedSkeleton):
        pass

    data = WaveData(lon=range(10), lat=range(10))
    data.set_hs2(1)
    data.set_tp2(10)
    data.set_dirp2(90)

    return data


@pytest.fixture
def freq_dirs_no_std():
    @add_coord("directions")
    @add_coord("frequency")
    @add_datavar("hs")
    class WaveData(PointSkeleton):
        pass

    data = WaveData(x=range(10), y=range(10), frequency=[1, 2, 3], directions=[6, 7, 8])
    data.set_hs(1)

    return data


@pytest.fixture
def freq_dirs_std():
    @add_direction()
    @add_frequency()
    @add_datavar(gp.wave.Hs("hs"))
    class WaveData(PointSkeleton):
        pass

    data = WaveData(lon=range(10), lat=range(10), freq=[1, 2, 3], dirs=[6, 7, 8])
    data.set_hs(1)
    return data


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
    data.set_uy(4)
    data.set_ux(3)
    data.set_swh(5)
    data.set_tp(15)
    data.set_dummy(6)
    return data


def test_no_std_name(wave_no_std):
    assert (
        _map_geo_parameter_to_ds_variable(
            "hs",
            wave_no_std.ds(),
            {},
            {},
            [],
            [],
            ignore_dir_ambiguity=False,
            verbose=False,
        )
        == "hs"
    )
    assert (
        _map_geo_parameter_to_ds_variable(
            "swh",
            wave_no_std.ds(),
            {},
            {},
            [],
            [],
            ignore_dir_ambiguity=False,
            verbose=False,
        )
        == "hs"
    )


def test_alias_to_std_name_mapping(wave2_std):
    assert (
        _map_geo_parameter_to_ds_variable(
            "hs",
            wave2_std.ds(),
            {},
            {},
            [],
            [],
            ignore_dir_ambiguity=False,
            verbose=False,
        )
        == "hs2"
    )
    assert (
        _map_geo_parameter_to_ds_variable(
            "swh",
            wave2_std.ds(),
            {},
            {},
            [],
            [],
            ignore_dir_ambiguity=False,
            verbose=False,
        )
        == "hs2"
    )


def test_given_aliases(wave2_std):
    assert (
        _map_geo_parameter_to_ds_variable(
            "hs5",
            wave2_std.ds(),
            aliases={"hs5": "hs2"},
            ds_aliases={},
            ignore_vars=[],
            only_vars=[],
            ignore_dir_ambiguity=False,
            verbose=False,
        )
        == "hs2"
    )
    assert (
        _map_geo_parameter_to_ds_variable(
            "swh5",
            wave2_std.ds(),
            aliases={"swh5": "hs2"},
            ds_aliases={},
            ignore_vars=[],
            only_vars=[],
            ignore_dir_ambiguity=False,
            verbose=False,
        )
        == "hs2"
    )


def test_gp_to_no_stdname(wave_no_std):
    assert (
        _map_geo_parameter_to_ds_variable(
            gp.wave.Hs,
            wave_no_std.ds(),
            {},
            {},
            [],
            [],
            ignore_dir_ambiguity=False,
            verbose=False,
        )
        == "hs"
    )
    assert (
        _map_geo_parameter_to_ds_variable(
            gp.wave.Hs("swh"),
            wave_no_std.ds(),
            {},
            {},
            [],
            [],
            ignore_dir_ambiguity=False,
            verbose=False,
        )
        == "hs"
    )


def test_gp_to_stdname(wave_std):
    assert (
        _map_geo_parameter_to_ds_variable(
            gp.wave.Hs,
            wave_std.ds(),
            {},
            {},
            ignore_vars=[],
            only_vars=[],
            ignore_dir_ambiguity=False,
            verbose=False,
        )
        == "hs"
    )
    assert (
        _map_geo_parameter_to_ds_variable(
            gp.wave.Hs("swh"),
            wave_std.ds(),
            {},
            {},
            [],
            [],
            ignore_dir_ambiguity=False,
            verbose=False,
        )
        == "hs"
    )


def test_get_similar(wave_std):
    assert (
        _map_similar_geo_parameter_to_ds_variable(
            gp.wave.Fp, wave_std.ds(), {}, {}, [], [], verbose=False
        )[0]
        == "tp"
    )
    assert (
        _map_similar_geo_parameter_to_ds_variable(
            "fp", wave_std.ds(), {}, {}, [], [], verbose=False
        )[0]
        == "tp"
    )


def test_get_components(wind_xy):
    assert _map_geo_parameter_to_components_in_ds(
        gp.wind.Wind, wind_xy.ds(), {}, {}, [], [], verbose=False
    )[0:2] == (
        "ux",
        "uy",
    )

    assert (
        _map_geo_parameter_to_components_in_ds(
            gp.wind.Wind, wind_xy.ds(), {}, {}, [], [], verbose=False
        )[-1]
    )
    assert (
        _map_geo_parameter_to_components_in_ds(
            gp.wind.Wind, wind_xy.ds(), {}, {}, [], [], verbose=False
        )[-2] is None
    )

    assert _map_geo_parameter_to_components_in_ds(
        gp.wind.WindDir,
        wind_xy.ds(),
        {},
        {},
        ignore_vars=[],
        only_vars=[],
        verbose=False,
    )[0:2] == (
        "ux",
        "uy",
    )

    assert (
        _map_geo_parameter_to_components_in_ds(
            gp.wind.WindDir, wind_xy.ds(), {}, {}, [], [], verbose=False
        )[-2]
        == "math"
    )

    assert _map_geo_parameter_to_components_in_ds(
        gp.wind.WindDirTo,
        wind_xy.ds(),
        {},
        {},
        ignore_vars=[],
        only_vars=[],
        verbose=False,
    )[0:2] == (
        "ux",
        "uy",
    )
    assert (
        _map_geo_parameter_to_components_in_ds(
            gp.wind.WindDirTo, wind_xy.ds(), {}, {}, [], [], verbose=False
        )[-2]
        == "math"
    )

    assert _map_geo_parameter_to_components_in_ds(
        "wind_speed", wind_xy.ds(), {}, {}, [], [], verbose=False
    )[0:2] == (
        "ux",
        "uy",
    )
