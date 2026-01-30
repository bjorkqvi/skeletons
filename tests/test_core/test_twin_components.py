from geo_skeletons.errors import ProjectionError
from geo_skeletons import PointSkeleton
import pytest
import geo_parameters as gp
from geo_skeletons.classes import Wind


def test_data_not_defined():
    assert PointSkeleton.add_datavar('u').core.find_twin_component('u') is None

def test_data_not_component():
    assert PointSkeleton.add_datavar(gp.wave.Hs('hs')).core.find_twin_component('hs') is None

def test_no_twin_present():
    assert PointSkeleton.add_datavar(gp.wind.XWind('u')).core.find_twin_component('u') is None

def test_twin_present():
    Wind.core.find_twin_component('u') == 'v'
    Wind.core.find_twin_component('v') == 'u'