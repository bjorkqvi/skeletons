from typing import Type

from geo_skeletons.errors import ProjectionError
from geo_skeletons import PointSkeleton
import pytest
import geo_parameters as gp
from geo_skeletons.classes import Wind

def test_meta_not_a_component():
    data = PointSkeleton.add_datavar(gp.wave.Hs('hs'))(lon=1, lat=5)
    data.set_hs(5)
    data.hs(wrt=(33,'W'))

def test_no_complement_component_present():
    data = PointSkeleton.add_datavar(gp.wind.XWind('u'))(lon=1, lat=5)
    data.set_u(5)
    with pytest.raises(ProjectionError):
        data.u(wrt=(33,'W'))


def test_mask():
    data = PointSkeleton.add_mask('land')(lon=1, lat=5)
    data.land_mask(wrt=(33,'W'))
    with pytest.raises(TypeError):
        data.land_points(wrt=(33,'W'))

def test_no_projection_present():
    data = Wind(x=5, y=6)
    data.set_u(10)
    data.set_v(30)
    with pytest.raises(ProjectionError):
        data.u(wrt=(33,'W'))

def test_magnitude():
    data = Wind(lon=5, lat=6)
    data.set_u(10)
    data.set_v(30)
    data.ff(wrt=(33,'W'))