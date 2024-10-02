from geo_skeletons import PointSkeleton
from geo_skeletons.decorators import add_datavar

import geo_parameters as gp

from geo_skeletons.decoders import map_ds_to_gp
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

    data = WaveData(lon=range(10), lat=range(10))
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

    data = WaveData(lon=range(10), lat=range(10))
    data.set_hs2(1)
    data.set_tp2(10)
    data.set_dirp2(90)

    return data


def test_no_std_name(wave_no_std):
    """Uses trivial mapping"""
    data_vars, coords = map_ds_to_gp(wave_no_std.ds())

    assert set(data_vars.keys()) == {'hs','tp','dirp'}
    assert set(data_vars.values()) == {'hs','tp','dirp'}
    assert coords.get('y') == gp.grid.Y
    assert coords.get('x') == gp.grid.X

def test_std_name(wave_std):
    """Uses trivial mapping"""
    data_vars, coords = map_ds_to_gp(wave_std.ds())

    assert set(data_vars.keys()) == {'hs','tp','dirp'}
    assert data_vars.get('hs') == gp.wave.Hs
    assert data_vars.get('tp') == gp.wave.Tp
    assert data_vars.get('dirp') == gp.wave.Dirp
    assert coords.get('lon') == gp.grid.Lon
    assert coords.get('lat') == gp.grid.Lat


    data_vars, coords = map_ds_to_gp(wave_std.ds(), decode_cf=False)

    assert set(data_vars.keys()) == {'hs','tp','dirp'}
    assert set(data_vars.values()) == {'hs','tp','dirp'}
    assert coords.get('lon') == 'lon'
    assert coords.get('lat') == 'lat'

def test_std_name_long_coord_name(wave_std):
    """Uses trivial mapping"""
    ds = wave_std.ds().rename_vars({'lon': 'longitude', 'lat':'latitude'})
    
    data_vars, coords = map_ds_to_gp(ds)

    assert set(data_vars.keys()) == {'hs','tp','dirp'}
    assert data_vars.get('hs') == gp.wave.Hs
    assert data_vars.get('tp') == gp.wave.Tp
    assert data_vars.get('dirp') == gp.wave.Dirp
    assert coords.get('longitude') == gp.grid.Lon
    assert coords.get('latitude') == gp.grid.Lat


    data_vars, coords = map_ds_to_gp(ds, decode_cf=False)

    assert set(data_vars.keys()) == {'hs','tp','dirp'}
    assert set(data_vars.values()) == {'hs','tp','dirp'}
    assert coords.get('longitude') == 'longitude'
    assert coords.get('latitude') == 'latitude'