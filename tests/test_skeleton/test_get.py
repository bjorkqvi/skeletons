from geo_skeletons import PointSkeleton, GriddedSkeleton
import pytest
from geo_skeletons.errors import GridError, UnknownVariableError, DirTypeError, SkeletonError
import geo_parameters as gp
import numpy as np

def test_get_wrong_type():
    points = PointSkeleton.add_datavar('hs')(x=(10,29), y=(30,40))
    points.set('hs', [5,6])
    with pytest.raises(TypeError):
        points.get(5, [5,6])

def test_get_wrong_gp():
    points = PointSkeleton.add_datavar('hs')(x=(10,29), y=(30,40))
    points.set('hs', [5,6])
    with pytest.raises(UnknownVariableError):
        points.get(gp.wave.Tp, [5,6])

def test_get_wrong_dir_type():
    points = PointSkeleton.add_datavar(gp.wave.Dirm)(x=(10,29), y=(30,40))
    points.set('dirm', [5,6])
    with pytest.raises(DirTypeError):
        points.get('dirm', [5,6], dir_type='form')

def test_get_dirtype_for_magnitude():
    points = PointSkeleton.add_datavar('ux').add_datavar('uy').add_magnitude('u', x='ux',y='uy')(x=(10,29), y=(30,40))
    with pytest.raises(SkeletonError): # Can't calculate components since direction not set
        points.set('u', [5,6])
    points.set('ux', [5,6])
    points.set('ux', [2,3])
    with pytest.raises(DirTypeError):
        points.get('u', [5,6], dir_type='from')
