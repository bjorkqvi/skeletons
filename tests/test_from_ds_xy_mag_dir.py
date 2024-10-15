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
    data = PointSkeleton.from_ds(wind_magdir.ds())
    breakpoint()
