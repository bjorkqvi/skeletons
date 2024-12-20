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
    _map_inverse_geo_parameter_to_ds_variable,
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
            "hs", wave_no_std.ds(), {}, {}, [], [], verbose=False
        )
        == "hs"
    )
    assert (
        _map_geo_parameter_to_ds_variable(
            "swh", wave_no_std.ds(), {}, {}, [], [], verbose=False
        )
        == "hs"
    )


def test_alias_to_std_name_mapping(wave2_std):
    assert (
        _map_geo_parameter_to_ds_variable(
            "hs", wave2_std.ds(), {}, {}, [], [], verbose=False
        )
        == "hs2"
    )
    assert (
        _map_geo_parameter_to_ds_variable(
            "swh", wave2_std.ds(), {}, {}, [], [], verbose=False
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
            verbose=False,
        )
        == "hs2"
    )


def test_gp_to_no_stdname(wave_no_std):
    assert (
        _map_geo_parameter_to_ds_variable(
            gp.wave.Hs, wave_no_std.ds(), {}, {}, [], [], verbose=False
        )
        == "hs"
    )
    assert (
        _map_geo_parameter_to_ds_variable(
            gp.wave.Hs("swh"), wave_no_std.ds(), {}, {}, [], [], verbose=False
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
            verbose=False,
        )
        == "hs"
    )
    assert (
        _map_geo_parameter_to_ds_variable(
            gp.wave.Hs("swh"), wave_std.ds(), {}, {}, [], [], verbose=False
        )
        == "hs"
    )


def test_get_inverse(wave_std):
    assert (
        _map_inverse_geo_parameter_to_ds_variable(
            gp.wave.Fp, wave_std.ds(), {}, {}, [], [], verbose=False
        )[0]
        == "tp"
    )
    assert (
        _map_inverse_geo_parameter_to_ds_variable(
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
        is None
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
        )[-1]
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
        )[-1]
        == "math"
    )

    assert _map_geo_parameter_to_components_in_ds(
        "wind_speed", wind_xy.ds(), {}, {}, [], [], verbose=False
    )[0:2] == (
        "ux",
        "uy",
    )
    # assert _map_geo_parameter_to_components_in_ds("fp", wave_std.ds()) == "tp"


# def test_std_name(wave_std):
#     """Uses trivial mapping"""
#     data_vars, coords = map_ds_to_gp(wave_std.ds())

#     assert set(data_vars.keys()) == {"hs", "tp", "dirp"}
#     assert data_vars.get("hs").is_same(gp.wave.Hs)
#     assert data_vars.get("tp").is_same(gp.wave.Tp)
#     assert data_vars.get("dirp").is_same(gp.wave.Dirp)
#     assert coords.get("lon").is_same(gp.grid.Lon)
#     assert coords.get("lat").is_same(gp.grid.Lat)

#     data_vars, coords = map_ds_to_gp(wave_std.ds(), decode_cf=False)

#     assert data_vars.get("hs").is_same(gp.wave.Hs)
#     assert data_vars.get("tp").is_same(gp.wave.Tp)
#     assert data_vars.get("dirp") == "dirp"
#     assert coords.get("lon").is_same(gp.grid.Lon)
#     assert coords.get("lat").is_same(gp.grid.Lat)


# def test_std_name_long_coord_name(wave_std):
#     """Uses trivial mapping"""
#     ds = wave_std.ds().rename_vars({"lon": "longitude", "lat": "latitude"})

#     data_vars, coords = map_ds_to_gp(ds)

#     assert set(data_vars.keys()) == {"hs", "tp", "dirp"}
#     assert data_vars.get("hs").is_same(gp.wave.Hs)
#     assert data_vars.get("tp").is_same(gp.wave.Tp)
#     assert data_vars.get("dirp").is_same(gp.wave.Dirp)
#     assert coords.get("longitude").is_same(gp.grid.Lon)
#     assert coords.get("latitude").is_same(gp.grid.Lat)

#     data_vars, coords = map_ds_to_gp(ds, decode_cf=False)

#     assert data_vars.get("hs").is_same(gp.wave.Hs)
#     assert data_vars.get("tp").is_same(gp.wave.Tp)
#     assert data_vars.get("dirp") == "dirp"
#     assert coords.get("longitude").is_same(gp.grid.Lon)
#     assert coords.get("latitude").is_same(gp.grid.Lat)


# def test_std_name_long_coord_name_alias(wave_std):
#     """Uses trivial mapping"""
#     ds = wave_std.ds().rename_vars({"lon": "longitude_degrees", "lat": "latitude"})

#     data_vars, coords = map_ds_to_gp(ds, aliases={"longitude_degrees": gp.grid.Lon})

#     assert set(data_vars.keys()) == {"hs", "tp", "dirp"}
#     assert data_vars.get("hs").is_same(gp.wave.Hs)
#     assert data_vars.get("tp").is_same(gp.wave.Tp)
#     assert data_vars.get("dirp").is_same(gp.wave.Dirp)
#     assert coords.get("longitude_degrees").is_same(gp.grid.Lon)
#     assert coords.get("latitude").is_same(gp.grid.Lat)


# def test_no_std_name_gridded(wave2_no_std):
#     """Uses trivial mapping"""
#     data_vars, coords = map_ds_to_gp(wave2_no_std.ds())

#     assert set(data_vars.keys()) == {"hs2", "tp2", "dirp2"}
#     assert set(data_vars.values()) == {"hs2", "tp2", "dirp2"}
#     assert coords.get("y").is_same(gp.grid.Y)
#     assert coords.get("x").is_same(gp.grid.X)


# def test_std_name_gridded(wave2_std):
#     """Uses trivial mapping"""
#     data_vars, coords = map_ds_to_gp(wave2_std.ds())

#     assert set(data_vars.keys()) == {"hs2", "tp2", "dirp2"}
#     assert data_vars.get("hs2").is_same(gp.wave.Hs)
#     assert data_vars.get("tp2").is_same(gp.wave.Tp)
#     assert data_vars.get("dirp2").is_same(gp.wave.Dirp)
#     assert coords.get("lon").is_same(gp.grid.Lon)
#     assert coords.get("lat").is_same(gp.grid.Lat)

#     data_vars, coords = map_ds_to_gp(wave2_std.ds(), decode_cf=False)

#     assert set(data_vars.keys()) == {"hs2", "tp2", "dirp2"}
#     assert set(data_vars.values()) == {"hs2", "tp2", "dirp2"}
#     assert coords.get("lon").is_same(gp.grid.Lon)
#     assert coords.get("lat").is_same(gp.grid.Lat)


# def test_std_name_long_coord_name_gridded(wave2_std):
#     """Uses trivial mapping"""
#     ds = wave2_std.ds().rename_vars({"lon": "longitude", "lat": "latitude"})

#     data_vars, coords = map_ds_to_gp(ds)

#     assert set(data_vars.keys()) == {"hs2", "tp2", "dirp2"}
#     assert data_vars.get("hs2").is_same(gp.wave.Hs)
#     assert data_vars.get("tp2").is_same(gp.wave.Tp)
#     assert data_vars.get("dirp2").is_same(gp.wave.Dirp)
#     assert coords.get("longitude").is_same(gp.grid.Lon)
#     assert coords.get("latitude").is_same(gp.grid.Lat)

#     data_vars, coords = map_ds_to_gp(ds, decode_cf=False)

#     assert set(data_vars.keys()) == {"hs2", "tp2", "dirp2"}
#     assert set(data_vars.values()) == {"hs2", "tp2", "dirp2"}
#     assert coords.get("longitude").is_same(gp.grid.Lon)
#     assert coords.get("latitude").is_same(gp.grid.Lat)


# def test_std_name_long_coord_name_alias_gridded(wave2_std):
#     """Uses trivial mapping"""
#     ds = wave2_std.ds().rename_vars({"lon": "longitude_degrees", "lat": "latitude"})

#     data_vars, coords = map_ds_to_gp(ds, aliases={"longitude_degrees": gp.grid.Lon})

#     assert set(data_vars.keys()) == {"hs2", "tp2", "dirp2"}
#     assert data_vars.get("hs2").is_same(gp.wave.Hs)
#     assert data_vars.get("tp2").is_same(gp.wave.Tp)
#     assert data_vars.get("dirp2").is_same(gp.wave.Dirp)
#     assert coords.get("longitude_degrees").is_same(gp.grid.Lon)
#     assert coords.get("latitude").is_same(gp.grid.Lat)


# def test_no_std_name_gridded_aliases(wave2_no_std):
#     """Uses trivial mapping"""
#     data_vars, coords = map_ds_to_gp(
#         wave2_no_std.ds(), aliases={"hs2": gp.wave.Hs("hsig")}
#     )

#     assert set(data_vars.keys()) == {"hs2", "tp2", "dirp2"}
#     assert data_vars.get("tp2") == "tp2"
#     assert data_vars.get("dirp2") == "dirp2"
#     assert gp.is_gp_instance(data_vars.get("hs2"))
#     assert data_vars.get("hs2").name == "hsig"

#     assert coords.get("y").is_same(gp.grid.Y)
#     assert coords.get("x").is_same(gp.grid.X)


# def test_std_name_gridded_aliases_keep_ds_names(wave2_std):
#     """Uses trivial mapping"""
#     data_vars, coords = map_ds_to_gp(
#         wave2_std.ds(), aliases={"hs2": gp.wave.Hs("hsig")}, keep_ds_names=True
#     )

#     assert set(data_vars.keys()) == {"hs2", "tp2", "dirp2"}
#     assert gp.is_gp_instance(data_vars.get("hs2"))
#     assert gp.is_gp_instance(data_vars.get("tp2"))
#     assert gp.is_gp_instance(data_vars.get("dirp2"))
#     assert data_vars.get("hs2").name == "hsig"
#     assert data_vars.get("tp2").name == "tp2"
#     assert data_vars.get("dirp2").name == "dirp2"
#     assert gp.is_gp_instance(coords.get("lon"))
#     assert gp.is_gp_instance(coords.get("lat"))
#     assert coords.get("lat").name == "lat"
#     assert coords.get("lon").name == "lon"


# def test_std_name_long_coord_name_alias_gridded_keep_ds_names(wave2_std):
#     """Uses trivial mapping"""
#     ds = wave2_std.ds().rename_vars({"lon": "longitude_degrees", "lat": "latitude"})

#     data_vars, coords = map_ds_to_gp(
#         ds, aliases={"longitude_degrees": gp.grid.Lon}, keep_ds_names=True
#     )

#     assert set(data_vars.keys()) == {"hs2", "tp2", "dirp2"}
#     assert gp.is_gp_instance(data_vars.get("hs2"))
#     assert gp.is_gp_instance(data_vars.get("tp2"))
#     assert gp.is_gp_instance(data_vars.get("dirp2"))
#     assert data_vars.get("hs2").name == "hs2"
#     assert data_vars.get("tp2").name == "tp2"
#     assert data_vars.get("dirp2").name == "dirp2"

#     assert gp.is_gp_instance(coords.get("longitude_degrees"))
#     assert gp.is_gp_instance(coords.get("latitude"))
#     # ds-names never kept for coordinates
#     assert coords.get("latitude").name == "lat"
#     assert coords.get("longitude_degrees").name == "lon"


# def test_freq_dirs_no_std_name(freq_dirs_no_std):
#     data_vars, coords = map_ds_to_gp(freq_dirs_no_std.ds())

#     assert data_vars.get("hs").is_same(gp.wave.Hs)
#     assert coords.get("y").is_same(gp.grid.Y)
#     assert coords.get("x").is_same(gp.grid.X)
#     assert coords.get("frequency").is_same(gp.wave.Freq)
#     assert coords.get("directions").is_same(gp.wave.Dirs)


# def test_freq_dirs_std_name(freq_dirs_std):
#     data_vars, coords = map_ds_to_gp(freq_dirs_std.ds())
#     assert set(data_vars.keys()) == {"hs"}
#     assert data_vars.get("hs").is_same(gp.wave.Hs)
#     assert coords.get("lon").is_same(gp.grid.Lon)
#     assert coords.get("lat").is_same(gp.grid.Lat)
#     assert coords.get("freq").is_same(gp.wave.Freq)
#     assert coords.get("dirs").is_same(gp.wave.Dirs)
