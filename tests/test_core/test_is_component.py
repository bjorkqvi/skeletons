from geo_skeletons import PointSkeleton
import pytest
import geo_parameters as gp
from geo_skeletons.classes import Wind


def test_datavar_not_component():
    assert not Wind.add_datavar('dummy').core.is_component('dummy')
    assert not Wind.add_datavar(gp.wave.Hs).core.is_component('hs')

def test_direction_not_component():
    assert not Wind.core.is_component('ff')

def test_magnitude_not_component():
    assert not Wind.core.is_component('ff')

def test_gp_component():
    assert Wind.core.is_component('u')
    assert Wind.core.is_component('v')

def test_non_gp_component():
    cls = PointSkeleton.add_datavar(['u','v','w']).add_magnitude('wind',x='u', y='v')
    assert cls.core.is_component('u')
    assert cls.core.is_component('v')
    assert not cls.core.is_component('w')
    with pytest.raises(ValueError):
        cls.core.is_component('g')