from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_datavar, add_frequency, add_direction

import geo_parameters as gp

from geo_skeletons.decoders import identify_core_in_ds
import pytest

def test_utm_zone_not_None():
    grid = GriddedSkeleton(lon=(0, 1), lat=(10, 11))
    grid.set_spacing(nx=10, ny=10)


def test_coord_meta():
    @add_direction()
    @add_frequency()
    @add_datavar(gp.wave.Hs('hs'))
    class WaveData(PointSkeleton):
        pass

    @add_direction(gp.wave.DirsTo)
    @add_frequency()
    @add_datavar(gp.wave.Hs('hs'))
    class WaveGrid(PointSkeleton):
        pass


    data = WaveData(lon=range(10), lat=range(10), freq=[1,2,3], dirs=[6,7,8]) 
    grid = WaveData(lon=range(10), lat=range(10), freq=[1,2,3], dirs=[6,7,8]) 
    
    data.set_hs(1)
    grid.set_hs(1)
    assert set(data.meta.meta_dict().keys()) == {'_global_', 'lat','lon','inds','freq','dirs','hs'}