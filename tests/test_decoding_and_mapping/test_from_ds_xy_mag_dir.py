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
import numpy as np


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


@pytest.fixture
def wind_class_xy():
    @add_datavar(gp.wind.YWind("uy"))
    @add_datavar(gp.wind.XWind("ux"))
    class WindData(PointSkeleton):
        pass

    return WindData


@pytest.fixture
def wind_class_magdir():
    @add_datavar(gp.wind.WindDir("ud"))
    @add_datavar(gp.wind.Wind("u"))
    class WindData(PointSkeleton):
        pass

    return WindData


def test_magdir(wind_magdir):
    data = PointSkeleton.from_ds(wind_magdir.ds(), dynamic=True)
    assert set(data.core.data_vars()) == {"x_wind", "y_wind", "tp", "hs", "dummy"}
    assert set(data.core.magnitudes()) == {"ff"}
    assert set(data.core.directions()) == {"dd"}
    np.testing.assert_array_almost_equal(data.ff(), 10)
    np.testing.assert_array_almost_equal(data.dd(), 100)
    np.testing.assert_array_almost_equal(data.dd(dir_type="from"), 100)
    np.testing.assert_array_almost_equal(data.dd(dir_type="to"), 280)


def test_magdirto(wind_magdirto):
    data = PointSkeleton.from_ds(wind_magdirto.ds(), dynamic=True)
    assert set(data.core.data_vars()) == {"x_wind", "y_wind", "tp", "hs", "dummy"}
    assert set(data.core.magnitudes()) == {"ff"}
    assert set(data.core.directions()) == {"dd"}

    np.testing.assert_array_almost_equal(data.ff(), 10)
    np.testing.assert_array_almost_equal(data.dd(), 280)
    np.testing.assert_array_almost_equal(data.dd(dir_type="from"), 280)
    np.testing.assert_array_almost_equal(data.dd(dir_type="to"), 100)


def test_xy(wind_xy):
    data = PointSkeleton.from_ds(wind_xy.ds(), dynamic=True)
    assert set(data.core.data_vars()) == {"x_wind", "y_wind", "tp", "hs", "dummy"}
    assert set(data.core.magnitudes()) == {"ff"}
    assert set(data.core.directions()) == {"dd"}

    np.testing.assert_array_almost_equal(data.x_wind(), 3)
    np.testing.assert_array_almost_equal(data.y_wind(), 4)
    np.testing.assert_array_almost_equal(data.ff(), 5)
    np.testing.assert_array_almost_equal(data.dd(), 216.86989765)
    np.testing.assert_array_almost_equal(data.dd(dir_type="from"), 216.86989765)
    np.testing.assert_array_almost_equal(data.dd(dir_type="to"), 216.86989765 - 180.0)


def test_class_xy(wind_class_xy, wind_xy, wind_magdir, wind_magdirto):
    data = wind_class_xy.from_ds(
        wind_xy.ds(), dynamic=True, ignore_vars=["tp", "swh", "dummy"]
    )

    assert set(data.core.data_vars()) == {"ux", "uy"}
    assert set(data.core.magnitudes()) == {"ff"}
    assert set(data.core.directions()) == {"dd"}
    np.testing.assert_array_almost_equal(data.ff(), 5)
    np.testing.assert_array_almost_equal(data.ux(), 3)
    np.testing.assert_array_almost_equal(data.uy(), 4)
    np.testing.assert_array_almost_equal(data.dd(), 216.86989765)
    np.testing.assert_array_almost_equal(data.dd(dir_type="from"), 216.86989765)
    np.testing.assert_array_almost_equal(data.dd(dir_type="to"), 216.86989765 - 180.0)

    data = wind_class_xy.from_ds(
        wind_magdir.ds(), dynamic=True, ignore_vars=["tp", "swh", "dummy"]
    )
    assert set(data.core.data_vars()) == {"ux", "uy"}
    assert set(data.core.magnitudes()) == {"ff"}
    assert set(data.core.directions()) == {"dd"}
    np.testing.assert_array_almost_equal(data.ff(), 10)
    np.testing.assert_array_almost_equal(data.dd(), 100)
    np.testing.assert_array_almost_equal(data.dd(dir_type="from"), 100)
    np.testing.assert_array_almost_equal(data.dd(dir_type="to"), 280)

    data = wind_class_xy.from_ds(
        wind_magdirto.ds(), dynamic=True, ignore_vars=["tp", "swh", "dummy"]
    )
    assert set(data.core.data_vars()) == {"ux", "uy"}
    assert set(data.core.magnitudes()) == {"ff"}
    assert set(data.core.directions()) == {"dd"}
    np.testing.assert_array_almost_equal(data.ff(), 10)
    np.testing.assert_array_almost_equal(data.dd(), 280)
    np.testing.assert_array_almost_equal(data.dd(dir_type="from"), 280)
    np.testing.assert_array_almost_equal(data.dd(dir_type="to"), 100)


def test_class_magdir(wind_class_magdir, wind_xy, wind_magdir, wind_magdirto):
    data = wind_class_magdir.from_ds(
        wind_magdir.ds(), dynamic=True, ignore_vars=["tp", "swh", "dummy"]
    )
    assert set(data.core.data_vars()) == {"u", "ud"}
    assert set(data.core.magnitudes()) == set({})
    assert set(data.core.directions()) == set({})
    np.testing.assert_array_almost_equal(data.u(), 10)
    np.testing.assert_array_almost_equal(data.ud(), 100)
    np.testing.assert_array_almost_equal(data.ud(dir_type="from"), 100)
    np.testing.assert_array_almost_equal(data.ud(dir_type="to"), 280)

    data = wind_class_magdir.from_ds(
        wind_magdirto.ds(), dynamic=True, ignore_vars=["tp", "swh", "dummy"]
    )
    assert set(data.core.data_vars()) == {"u", "ud"}
    assert set(data.core.magnitudes()) == set({})
    assert set(data.core.directions()) == set({})
    np.testing.assert_array_almost_equal(data.u(), 10)
    np.testing.assert_array_almost_equal(data.ud(), 280)
    np.testing.assert_array_almost_equal(data.ud(dir_type="from"), 280)
    np.testing.assert_array_almost_equal(data.ud(dir_type="to"), 100)

    data = wind_class_magdir.from_ds(
        wind_xy.ds(), dynamic=True, ignore_vars=["tp", "swh", "dummy"]
    )
    assert set(data.core.data_vars()) == {"u", "ud"}
    assert set(data.core.magnitudes()) == set({})
    assert set(data.core.directions()) == set({})
    np.testing.assert_array_almost_equal(data.u(), 5)
    np.testing.assert_array_almost_equal(data.ud(), 216.86989765)
    np.testing.assert_array_almost_equal(data.ud(dir_type="from"), 216.86989765)
    np.testing.assert_array_almost_equal(data.ud(dir_type="to"), 216.86989765 - 180.0)
