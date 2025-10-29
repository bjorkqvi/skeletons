from geo_skeletons import PointSkeleton
from geo_skeletons.decorators import add_datavar, add_coord
import numpy as np


def test_add_gp_trivial_ind():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(PointSkeleton):
        pass

    points = Expanded(x=0, y=4, z=[0, 1, 2])
    points.set_hs(0)
    assert points.size("grid") == (1,)
    assert points.size("gridpoint") == (3,)
    assert points.size("grid", squeeze=True) == (1,)
    assert points.size("gridpoint", squeeze=True) == (3,)
    assert points.shape("hs") == (1, 3)
    assert points.shape("hs", squeeze=True) == (3,)


def test_add_gp_all_trivial():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(PointSkeleton):
        pass

    points = Expanded(x=0, y=4, z=1)
    points.set_hs(0)
    assert points.size("grid") == (1,)
    assert points.size("grid", squeeze=True) == (1,)
    assert points.size("gridpoint") == (1,)
    assert points.size("gridpoint", squeeze=True) == (1,)
    assert points.shape("hs") == (1, 1)
    assert points.shape("hs", squeeze=True) == (1,)


def test_add_g():
    @add_datavar("hs")
    @add_coord("z", grid_coord=True)
    class Expanded(PointSkeleton):
        pass

    points = Expanded(x=0, y=4, z=[0, 1, 2])
    points.set_hs(0)
    assert points.size("grid") == (1, 3)
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint") == ()
    assert points.size("gridpoint", squeeze=True) == ()
    assert points.shape("hs") == (1, 3)
    assert points.shape("hs", squeeze=True) == (3,)
