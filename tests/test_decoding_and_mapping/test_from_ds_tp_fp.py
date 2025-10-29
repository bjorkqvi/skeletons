from geo_skeletons.point_skeleton import PointSkeleton
from geo_skeletons.gridded_skeleton import GriddedSkeleton
from geo_skeletons.decorators import add_coord, add_datavar
import numpy as np
import geo_parameters as gp
import pytest


@pytest.fixture
def tp_gp():
    @add_datavar(gp.wave.Tp)
    class WaveData(PointSkeleton):
        pass

    return WaveData


@pytest.fixture
def fp_gp():
    @add_datavar(gp.wave.Fp)
    class WaveData(PointSkeleton):
        pass

    return WaveData


@pytest.fixture
def tp():
    @add_datavar("tp")
    class WaveData(PointSkeleton):
        pass

    return WaveData


@pytest.fixture
def fp():
    @add_datavar("fp")
    class WaveData(PointSkeleton):
        pass

    return WaveData


def test_no_gp_no_gp(tp, fp):
    tpdata = tp(lon=0, lat=0)
    tpdata.set_tp(10)

    data = fp.from_ds(tpdata.ds())
    assert data.core.data_vars() == ["fp"]
    np.testing.assert_array_almost_equal(data.fp(), 0.1)

    fpdata = fp(lon=0, lat=0)
    fpdata.set_fp(0.1)

    data = tp.from_ds(fpdata.ds())
    assert data.core.data_vars() == ["tp"]
    np.testing.assert_array_almost_equal(data.tp(), 10)


def test_gp_no_gp(tp_gp, fp):
    tpdata = tp_gp(lon=0, lat=0)
    tpdata.set_tp(10)

    data = fp.from_ds(tpdata.ds())
    assert data.core.data_vars() == ["fp"]
    np.testing.assert_array_almost_equal(data.fp(), 0.1)


def test_gp_no_gp2(tp, fp_gp):
    fpdata = fp_gp(lon=0, lat=0)
    fpdata.set_fp(0.1)

    data = tp.from_ds(fpdata.ds())
    assert data.core.data_vars() == ["tp"]
    np.testing.assert_array_almost_equal(data.tp(), 10)


def test_no_gp_to_gp(tp_gp, fp):
    fpdata = fp(lon=0, lat=0)
    fpdata.set_fp(0.1)

    data = tp_gp.from_ds(fpdata.ds())
    assert data.core.data_vars() == ["tp"]
    np.testing.assert_array_almost_equal(data.tp(), 10)


def test_no_gp_to_gp2(tp, fp_gp):
    tpdata = tp(lon=0, lat=0)
    tpdata.set_tp(10)

    data = fp_gp.from_ds(tpdata.ds())
    assert data.core.data_vars() == ["fp"]
    np.testing.assert_array_almost_equal(data.fp(), 0.1)
