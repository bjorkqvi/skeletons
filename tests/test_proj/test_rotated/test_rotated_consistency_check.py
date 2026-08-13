from geo_skeletons.classes import WindGrid
import pytest
from geo_skeletons.errors import ProjectionError
import geo_parameters as gp
import numpy as np
def test_magnitude():
    wind = WindGrid(lon=0, lat=0)

    with pytest.raises(TypeError):
        wind.set_ff(0, rotated=True)
    
    with pytest.raises(ProjectionError):
        wind.set('ff',0, rotated=True)

    with pytest.raises(ProjectionError):
        wind.ff(rotated=True)

    with pytest.raises(ProjectionError):
        wind.get('ff', rotated=True)

def test_datavar():
    wind = WindGrid.add_datavar(gp.wave.Hs)(lon=0, lat=0)

    with pytest.raises(ProjectionError):
        wind.set_hs(0, rotated=True)
    
    with pytest.raises(ProjectionError):
        wind.set('hs',0, rotated=True)

    with pytest.raises(ProjectionError):
        wind.hs(rotated=True)


    with pytest.raises(ProjectionError):
        wind.get('hs',rotated=True)


def test_mask():
    wind = WindGrid.add_mask('land', opposite_name='sea')(lon=0, lat=0)

    with pytest.raises(TypeError):
        wind.set_land_mask(0, rotated=True)
    
    with pytest.raises(ProjectionError):
        wind.set('land_mask',0, rotated=True)

    with pytest.raises(ProjectionError):
        wind.land_mask(rotated=True)

    with pytest.raises(ProjectionError):
        wind.get('land_mask', rotated=True)

    with pytest.raises(ProjectionError):
        wind.land_points(rotated=True)

    with pytest.raises(ValueError):
        wind.get('land_points', rotated=True)



