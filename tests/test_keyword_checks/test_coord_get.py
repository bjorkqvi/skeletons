from geo_skeletons import PointSkeleton, GriddedSkeleton

import pytest


def test_point():
    data = PointSkeleton(lon=2, lat=(6,10))
    assert data.x() is not None
    assert data.lon(native=True) is not None
    assert data.x(strict=True) is None
    with pytest.raises(TypeError):
        data.x(stcrit=True)# Spelling wrong

    assert data.lon(inds=0)[0] == 2
    assert data.lon(inds=0, method='nearest')[0] == 2
    with pytest.raises(TypeError):
        data.lon(method='nearest') # Nothing o slice, so don't need keywords for slicing

def test_gridded():
    data = GriddedSkeleton(lon=(2,3), lat=(6,10))
    assert data.x() is None
    assert data.lon(native=True) is not None
    assert data.x(strict=True) is None
    with pytest.raises(TypeError):
        data.x(stcrit=True)# Spelling wrong

    with pytest.raises(TypeError):
        data.lon(method='nearest') # Nothing o slice, so don't need keywords for slicing


def test_point_add_datavar():
    data = PointSkeleton.add_datavar('c')(lon=(2,3), lat=(6,10))
    data.set_c((10,11))
    assert data.c(inds=0)[0] == 10
    with pytest.raises(ValueError):
        data.c(inds=0, g=5)
    with pytest.raises(TypeError):
        data.lon(method='nearest') # Nothing o slice, so don't need keywords for slicing
