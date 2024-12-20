from geo_skeletons import PointSkeleton, GriddedSkeleton
from geo_skeletons.decorators import (
    add_datavar,
    add_time,
    add_frequency,
    add_direction,
    add_coord,
)

from geo_skeletons.decoders.ds_decoders import _map_ds_variable_to_geo_parameter
import geo_parameters as gp


def test_only_strings():
    @add_datavar("u")
    class WindData(PointSkeleton):
        pass

    data = WindData(lon=range(10), lat=range(10))
    data.set_u(3)
    ds = data.ds()

    assert (
        _map_ds_variable_to_geo_parameter(
            "u", ds=ds, aliases={}, decode_cf=False, keep_ds_names=False, verbose=False
        )
        == "u"
    )
    assert (
        _map_ds_variable_to_geo_parameter(
            "u",
            ds=ds,
            aliases={"u": "wind"},
            decode_cf=False,
            keep_ds_names=False,
            verbose=False,
        )
        == "wind"
    )
    assert (
        _map_ds_variable_to_geo_parameter(
            "u",
            ds=ds,
            aliases={"u": "wind"},
            decode_cf=False,
            keep_ds_names=True,
            verbose=False,
        )
        == "wind"
    )
    assert (
        _map_ds_variable_to_geo_parameter(
            "u", ds=ds, aliases={}, decode_cf=False, keep_ds_names=True, verbose=False
        )
        == "u"
    )


def test_gp():
    @add_datavar(gp.wind.XWind("xwind"))
    class WindData(PointSkeleton):
        pass

    data = WindData(lon=range(10), lat=range(10))
    data.set_xwind(3)
    ds = data.ds()

    assert (
        _map_ds_variable_to_geo_parameter(
            "xwind",
            ds=ds,
            aliases={},
            decode_cf=False,
            keep_ds_names=False,
            verbose=False,
        )
        == "xwind"
    )
    assert (
        _map_ds_variable_to_geo_parameter(
            "xwind",
            ds=ds,
            aliases={"xwind": "u"},
            decode_cf=False,
            keep_ds_names=False,
            verbose=False,
        )
        == "u"
    )

    param = _map_ds_variable_to_geo_parameter(
        "xwind",
        ds=ds,
        aliases={"xwind": gp.wind.XWind},
        decode_cf=False,
        keep_ds_names=False,
        verbose=False,
    )
    assert param.is_same(gp.wind.XWind)
    assert param.name == "x_wind"

    param = _map_ds_variable_to_geo_parameter(
        "xwind",
        ds=ds,
        aliases={"xwind": gp.wind.XWind},
        decode_cf=False,
        keep_ds_names=True,
        verbose=False,
    )
    assert param.is_same(gp.wind.XWind)
    assert param.name == "xwind"

    param = _map_ds_variable_to_geo_parameter(
        "xwind",
        ds=ds,
        aliases={"xwind": gp.wind.XWind("u")},
        decode_cf=False,
        keep_ds_names=True,
        verbose=False,  # Doesn nothing when initialized parameter given in aliases
    )
    assert param.is_same(gp.wind.XWind)
    assert param.name == "u"

    # Match standard name
    param = _map_ds_variable_to_geo_parameter(
        "xwind", ds=ds, aliases={}, decode_cf=True, keep_ds_names=False, verbose=False
    )
    assert param.is_same(gp.wind.XWind)
    assert param.name == "x_wind"


def test_coord_alias():
    data = PointSkeleton(lon=range(10), lat=range(10))

    ds = data.ds()
    ds = ds.rename({"lon": "longitude"})

    param = _map_ds_variable_to_geo_parameter(
        "lat", ds=ds, aliases={}, decode_cf=False, keep_ds_names=False, verbose=False
    )
    assert param.is_same(gp.grid.Lat)
    assert param.name == "lat"

    param = _map_ds_variable_to_geo_parameter(
        "longitude",
        ds=ds,
        aliases={},
        decode_cf=True,
        keep_ds_names=False,
        verbose=False,
    )
    assert param.is_same(gp.grid.Lon)
    assert param.name == "lon"

    # Alias matching
    param = _map_ds_variable_to_geo_parameter(
        "longitude",
        ds=ds,
        aliases={},
        decode_cf=False,
        keep_ds_names=False,
        verbose=False,
    )
    assert param.is_same(gp.grid.Lon)
    assert param.name == "lon"


def test_hs():
    @add_datavar(gp.wave.Hs)
    class WaveData(GriddedSkeleton):
        pass

    data = WaveData(lon=0, lat=0)
    data.set_hs(1)
    ds = data.ds()
    param = _map_ds_variable_to_geo_parameter(
        "hs", ds=ds, aliases={}, decode_cf=False, keep_ds_names=False, verbose=False
    )
    #  Alias matching
    assert param.is_same(gp.wave.Hs)
    assert param.name == "hs"

    param = _map_ds_variable_to_geo_parameter(
        "hs", ds=ds, aliases={}, decode_cf=True, keep_ds_names=False, verbose=False
    )

    assert param.is_same(gp.wave.Hs)
    assert param.name == "hs"
    param = _map_ds_variable_to_geo_parameter(
        "hs",
        ds=ds,
        aliases={"hs": gp.wave.Hs("hsig")},
        decode_cf=True,
        keep_ds_names=False,
        verbose=False,
    )

    assert param.is_same(gp.wave.Hs)
    assert param.name == "hsig"
