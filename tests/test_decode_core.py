from geo_skeletons import PointSkeleton
from geo_skeletons.decorators import add_datavar

import geo_parameters as gp

from geo_skeletons.decoders import identify_core_in_ds
import pytest



@pytest.fixture
def wave_no_std():
    @add_datavar('dirp')
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

    data = WaveData(x=range(10), y=range(10))
    data.set_hs(1)
    data.set_tp(10)
    data.set_dirp(90)

    return data

@pytest.fixture
def wave2_no_std():
    @add_datavar('dirp2')
    @add_datavar("tp2")
    @add_datavar("hs2")
    class WaveData(PointSkeleton):
        pass

    data = WaveData(x=range(10), y=range(10))
    data.set_hs2(1)
    data.set_tp2(10)
    data.set_dirp2(90)

    return data

@pytest.fixture
def wave2_std():
    @add_datavar(gp.wave.Dirp('dirp2'))
    @add_datavar(gp.wave.Tp('tp2'))
    @add_datavar(gp.wave.Hs('hs2'))
    class WaveData(PointSkeleton):
        pass

    data = WaveData(x=range(10), y=range(10))
    data.set_hs2(1)
    data.set_tp2(10)
    data.set_dirp2(90)

    return data


def test_empty_core(wave_no_std, wave_std):
    data = PointSkeleton(lon=0, lat=0)
    core_vars = identify_core_in_ds(data, ds=wave_no_std.ds())
    assert core_vars == {}
    
    core_vars = identify_core_in_ds(data, ds=wave_std.ds())
    assert core_vars == {}

def test_core_with_gp(wave_no_std, wave_std):
    core_vars = identify_core_in_ds(wave_std, ds=wave_no_std.ds())
    assert set(core_vars.keys()) == {'hs','tp','dirp'}
    assert set(core_vars.values()) == {'hs','tp','dirp'}
    
    core_vars = identify_core_in_ds(wave_std, ds=wave_std.ds())
    assert set(core_vars.keys()) == {'hs','tp','dirp'}
    assert set(core_vars.values()) == {'hs','tp','dirp'}

def test_core_with_gp2(wave_no_std, wave_std, wave2_std):
    core_vars = identify_core_in_ds(wave2_std, ds=wave_no_std.ds())
    assert core_vars == {}
    
    core_vars = identify_core_in_ds(wave2_std, ds=wave_std.ds())
    assert set(core_vars.keys()) == {'hs2','tp2','dirp2'}
    assert set(core_vars.values()) == {'hs','tp','dirp'}

def test_core_without_gp2(wave_no_std, wave_std, wave2_no_std):
    core_vars = identify_core_in_ds(wave2_no_std, ds=wave_no_std.ds())
    assert core_vars == {}
    
    core_vars = identify_core_in_ds(wave2_no_std, ds=wave_std.ds())
    assert core_vars == {}

def test_core_with_gp2_explicit_dict_str(wave_no_std, wave_std, wave2_std):
    core_vars = identify_core_in_ds(wave2_std, ds=wave_no_std.ds(), core_to_ds={'hs2':'hs'})
    assert set(core_vars.keys()) == {'hs2'}
    assert set(core_vars.values()) == {'hs'}

    core_vars = identify_core_in_ds(wave2_std, ds=wave_std.ds())
    assert set(core_vars.keys()) == {'hs2','tp2','dirp2'}
    assert set(core_vars.values()) == {'hs','tp','dirp'}

def test_core_with_gp2_explicit_dict_gp(wave_no_std, wave_std, wave2_std):
    core_vars = identify_core_in_ds(wave2_std, ds=wave_no_std.ds(), core_to_ds={gp.wave.Hs:'hs'})
    assert set(core_vars.keys()) == {'hs2'}
    assert set(core_vars.values()) == {'hs'}

    core_vars = identify_core_in_ds(wave2_std, ds=wave_std.ds())
    assert set(core_vars.keys()) == {'hs2','tp2','dirp2'}
    assert set(core_vars.values()) == {'hs','tp','dirp'}

def test_core_with_gp2_explicit_dict_gp_wrong_ds_name(wave_no_std, wave_std, wave2_std):
    core_vars = identify_core_in_ds(wave2_std, ds=wave_no_std.ds(), core_to_ds={gp.wave.Hs:'hss'})
    assert set(core_vars.keys()) == set({})
    assert set(core_vars.values()) == set({})

    core_vars = identify_core_in_ds(wave2_std, ds=wave_std.ds())
    assert set(core_vars.keys()) == {'hs2','tp2','dirp2'}
    assert set(core_vars.values()) == {'hs','tp','dirp'}