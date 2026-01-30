from geo_skeletons.errors import ProjectionError
from geo_skeletons import PointSkeleton
import pytest
import geo_parameters as gp


def test_no_meta_found():
    data = PointSkeleton.add_datavar('u')(lon=1, lat=5)
    data.set_u(5)
    with pytest.raises(ProjectionError):
        data.u(rotated=True)

def test_meta_not_a_component():
    data = PointSkeleton.add_datavar(gp.wave.Hs('hs'))(lon=1, lat=5)
    data.set_hs(5)
    with pytest.raises(ProjectionError):
        data.hs(rotated=True)