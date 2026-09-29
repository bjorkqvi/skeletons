from geo_skeletons.classes import WindGrid
import pytest
from geo_skeletons.errors import ProjectionError

def test_values_are_projected():
    wind  = WindGrid(lon=0,lat=0)
    assert wind.proj.values_are_projected('u')
    assert wind.proj.values_are_projected('v')
    assert not wind.proj.values_are_projected('dd')

    # We should not be able to change this protected property
    wind.meta.append({'rotated_according_to': 'ggg'},'u')
    assert wind.proj.values_are_projected('u')
    
def test_lonlat():
    wind  = WindGrid(lon=0,lat=0)
    assert not wind.proj.values_are_projected('lon')
    assert not wind.proj.values_are_projected('lat')
    
def test_xy():   
    wind  = WindGrid(x=0,y=0)
    assert wind.proj.values_are_projected('x')
    assert wind.proj.values_are_projected('y')

def test_dummyvar():
    wind  = WindGrid.add_datavar('dummy')(lon=0,lat=0)
    with pytest.raises(ProjectionError):
        wind.proj.values_are_projected('dummy')