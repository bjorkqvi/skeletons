from geo_skeletons.point_skeleton import PointSkeleton
from geo_skeletons.gridded_skeleton import GriddedSkeleton
from geo_skeletons.decorators import add_coord, add_datavar
import numpy as np
import geo_parameters as gp
import pytest


@pytest.fixture
def wave():
    @add_datavar(gp.wave.Hs("Hm0b"))
    @add_datavar(gp.wave.Hs("Hm0a"))
    @add_datavar(gp.wave.Hs("Hm0"))
    class WindData(PointSkeleton):
        pass

    data = WindData(lon=range(10), lat=range(10))
    data.set_Hm0(5)
    data.set_Hm0a(6)
    data.set_Hm0b(7)

    return data


def test_conflict(wave):

    data = PointSkeleton.from_ds(wave.ds(), dynamic=True, verbose=True)
    # Hs added to core, but cannot be decoded
    assert data.hs(strict=True) is None


def test_keep_ds_names(wave):
    data = PointSkeleton.from_ds(wave.ds(), dynamic=True, keep_ds_names=True)
    np.testing.assert_array_almost_equal(data.Hm0(), 5)
    np.testing.assert_array_almost_equal(data.Hm0a(), 6)
    np.testing.assert_array_almost_equal(data.Hm0b(), 7)


def test_ignore_vars_keep_ds_names(wave):
    data = PointSkeleton.from_ds(
        wave.ds(), dynamic=True, ignore_vars=["Hm0a", "Hm0b"], keep_ds_names=True
    )
    np.testing.assert_array_almost_equal(data.Hm0(), 5)
    assert data.core.data_vars() == ["Hm0"]


def test_ignore_vars(wave):
    data = PointSkeleton.from_ds(wave.ds(), dynamic=True, ignore_vars=["Hm0a", "Hm0b"])
    np.testing.assert_array_almost_equal(data.hs(), 5)
    assert data.core.data_vars() == ["hs"]


def test_only_vars_leep_ds_names(wave):
    data = PointSkeleton.from_ds(
        wave.ds(), dynamic=True, only_vars=["Hm0"], keep_ds_names=True
    )
    np.testing.assert_array_almost_equal(data.Hm0(), 5)
    assert data.core.data_vars() == ["Hm0"]


def test_only_vars(wave):
    data = PointSkeleton.from_ds(wave.ds(), dynamic=True, only_vars=["Hm0"])
    np.testing.assert_array_almost_equal(data.hs(), 5)
    assert data.core.data_vars() == ["hs"]


def test_tie_breaker(wave):
    data = PointSkeleton.from_ds(
        wave.ds(), dynamic=True, ds_aliases={"Hm0": gp.wave.Hs}
    )
    np.testing.assert_array_almost_equal(data.hs(), 5)
    assert data.core.data_vars() == ["hs"]


def test_winner_by_default(wave):
    data = PointSkeleton.from_ds(
        wave.ds(),
        dynamic=True,
        ds_aliases={"Hm0a": gp.wave.Hs("hs_a"), "Hm0b": gp.wave.Hs("hs_b")},
    )
    np.testing.assert_array_almost_equal(data.hs(), 5)
    np.testing.assert_array_almost_equal(data.hs_a(), 6)
    np.testing.assert_array_almost_equal(data.hs_b(), 7)
    assert set(data.core.data_vars()) == {"hs", "hs_a", "hs_b"}
