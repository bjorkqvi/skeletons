from geo_skeletons.classes import WindGrid
import pytest
from geo_skeletons.errors import ProjectionError
import geo_parameters as gp
def test_magnitude():
    wind = WindGrid(lon=0, lat=0)

    with pytest.raises(TypeError):
        wind.set_ff(0, rotated=True)
    
    with pytest.raises(ProjectionError):
        wind.set('ff',0, rotated=True)

    with pytest.raises(ProjectionError):
        wind.ff(0, rotated=True)


def test_datavar():
    wind = WindGrid.add_datavar(gp.wave.Hs)(lon=0, lat=0)

    with pytest.raises(ProjectionError):
        wind.set_hs(0, rotated=True)
    
    with pytest.raises(ProjectionError):
        wind.set('hs',0, rotated=True)

    with pytest.raises(ProjectionError):
        wind.hs(0, rotated=True)