from geo_skeletons import GriddedSkeleton
from geo_skeletons.decorators import add_datavar, add_coord
import numpy as np


def test_add_gp_trivial_xy():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(x=0, y=4, z=[0, 1, 2])
    points.set_hs(0)
    assert points.size("grid") == (1, 1)
    assert points.size("gridpoint") == (3,)
    assert points.size("grid", squeeze=True) == (1,)
    assert points.size("gridpoint", squeeze=True) == (3,)
    assert points.shape("hs") == (1, 1, 3)
    assert points.shape("hs", squeeze=True) == (3,)

    points = Expanded(lon=0, lat=4, z=[0, 1, 2])
    points.set_hs(0)
    assert points.size("grid") == (1, 1)
    assert points.size("gridpoint") == (3,)
    assert points.size("grid", squeeze=True) == (1,)
    assert points.size("gridpoint", squeeze=True) == (3,)
    assert points.shape("hs") == (1, 1, 3)
    assert points.shape("hs", squeeze=True) == (3,)


def test_add_gp_all_trivial():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(x=0, y=4, z=1)
    points.set_hs(0)
    assert points.size("grid") == (1, 1)
    assert points.size("gridpoint") == (1,)
    assert points.size("grid", squeeze=True) == (1,)
    assert points.size("gridpoint", squeeze=True) == (1,)
    assert points.shape("hs") == (1, 1, 1)
    assert points.shape("hs", squeeze=True) == (1,)

    points = Expanded(lon=0, lat=4, z=1)
    points.set_hs(0)
    assert points.size("grid") == (1, 1)
    assert points.size("gridpoint") == (1,)
    assert points.size("grid", squeeze=True) == (1,)
    assert points.size("gridpoint", squeeze=True) == (1,)
    assert points.shape("hs") == (1, 1, 1)
    assert points.shape("hs", squeeze=True) == (1,)


def test_add_gp_x_trivial():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(x=0, y=[1, 2, 3], z=[5, 6])
    points.set_hs(0)
    assert points.size("grid") == (3, 1)
    assert points.size("gridpoint") == (2,)
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint", squeeze=True) == (2,)
    assert points.shape("hs") == (3, 1, 2)
    assert points.shape("hs", squeeze=True) == (3, 2)

    points = Expanded(lon=0, lat=[1, 2, 3], z=[5, 6])
    points.set_hs(0)
    assert points.size("grid") == (3, 1)
    assert points.size("gridpoint") == (2,)
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint", squeeze=True) == (2,)
    assert points.shape("hs") == (3, 1, 2)
    assert points.shape("hs", squeeze=True) == (3, 2)


def test_add_gp_y_trivial():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(y=0, x=[1, 2, 3], z=[5, 6])
    points.set_hs(0)
    assert points.size("grid") == (1, 3)
    assert points.size("gridpoint") == (2,)
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint", squeeze=True) == (2,)
    assert points.shape("hs") == (1, 3, 2)
    assert points.shape("hs", squeeze=True) == (3, 2)

    points = Expanded(lat=0, lon=[1, 2, 3], z=[5, 6])
    points.set_hs(0)
    assert points.size("grid") == (1, 3)
    assert points.size("gridpoint") == (2,)
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint", squeeze=True) == (2,)
    assert points.shape("hs") == (1, 3, 2)
    assert points.shape("hs", squeeze=True) == (3, 2)


def test_add_gp_xz_trivial():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(x=0, y=[1, 2, 3], z=5)
    points.set_hs(0)
    assert points.size("grid") == (3, 1)
    assert points.size("gridpoint") == (1,)
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint", squeeze=True) == (1,)
    assert points.shape("hs") == (3, 1, 1)
    assert points.shape("hs", squeeze=True) == (3,)

    points = Expanded(lon=0, lat=[1, 2, 3], z=5)
    points.set_hs(0)
    assert points.size("grid") == (3, 1)
    assert points.size("gridpoint") == (1,)
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint", squeeze=True) == (1,)
    assert points.shape("hs") == (3, 1, 1)
    assert points.shape("hs", squeeze=True) == (3,)


def test_add_gp_yz_trivial():
    @add_datavar("hs")
    @add_coord("z")
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(y=0, x=[1, 2, 3], z=5)
    points.set_hs(0)
    assert points.size("grid") == (1, 3)
    assert points.size("gridpoint") == (1,)
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint", squeeze=True) == (1,)
    assert points.shape("hs") == (1, 3, 1)
    assert points.shape("hs", squeeze=True) == (3,)

    points = Expanded(lat=0, lon=[1, 2, 3], z=5)
    points.set_hs(0)
    assert points.size("grid") == (1, 3)
    assert points.size("gridpoint") == (1,)
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint", squeeze=True) == (1,)
    assert points.shape("hs") == (1, 3, 1)
    assert points.shape("hs", squeeze=True) == (3,)


## Grid
def test_add_g_trivial_xy():
    @add_datavar("hs")
    @add_coord("z", grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(x=0, y=4, z=[0, 1, 2])
    points.set_hs(0)
    assert points.size("grid") == (1, 1, 3)
    assert points.size("gridpoint") == ()
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint", squeeze=True) == ()
    assert points.shape("hs") == (1, 1, 3)
    assert points.shape("hs", squeeze=True) == (3,)

    points = Expanded(lon=0, lat=4, z=[0, 1, 2])
    points.set_hs(0)
    assert points.size("grid") == (1, 1, 3)
    assert points.size("gridpoint") == ()
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint", squeeze=True) == ()
    assert points.shape("hs") == (1, 1, 3)
    assert points.shape("hs", squeeze=True) == (3,)


def test_add_g_all_trivial():
    @add_datavar("hs")
    @add_coord("z", grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(x=0, y=4, z=1)
    points.set_hs(0)
    assert points.size("grid") == (1, 1, 1)
    assert points.size("gridpoint") == ()
    assert points.size("grid", squeeze=True) == (1,)
    assert points.size("gridpoint", squeeze=True) == ()
    assert points.shape("hs") == (1, 1, 1)
    assert points.shape("hs", squeeze=True) == (1,)

    points = Expanded(lon=0, lat=4, z=1)
    points.set_hs(0)
    assert points.size("grid") == (1, 1, 1)
    assert points.size("gridpoint") == ()
    assert points.size("grid", squeeze=True) == (1,)
    assert points.size("gridpoint", squeeze=True) == ()
    assert points.shape("hs") == (1, 1, 1)
    assert points.shape("hs", squeeze=True) == (1,)


def test_add_g_x_trivial():
    @add_datavar("hs")
    @add_coord("z", grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(x=0, y=[1, 2, 3], z=[5, 6])
    points.set_hs(0)
    assert points.size("grid") == (3, 1, 2)
    assert points.size("gridpoint") == ()
    assert points.size("grid", squeeze=True) == (3, 2)
    assert points.size("gridpoint", squeeze=True) == ()
    assert points.shape("hs") == (3, 1, 2)
    assert points.shape("hs", squeeze=True) == (3, 2)

    points = Expanded(lon=0, lat=[1, 2, 3], z=[5, 6])
    points.set_hs(0)
    assert points.size("grid") == (3, 1, 2)
    assert points.size("gridpoint") == ()
    assert points.size("grid", squeeze=True) == (3, 2)
    assert points.size("gridpoint", squeeze=True) == ()
    assert points.shape("hs") == (3, 1, 2)
    assert points.shape("hs", squeeze=True) == (3, 2)


def test_add_g_y_trivial():
    @add_datavar("hs")
    @add_coord("z", grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(y=0, x=[1, 2, 3], z=[5, 6])
    points.set_hs(0)
    assert points.size("grid") == (1, 3, 2)
    assert points.size("gridpoint") == ()
    assert points.size("grid", squeeze=True) == (3, 2)
    assert points.size("gridpoint", squeeze=True) == ()
    assert points.shape("hs") == (1, 3, 2)
    assert points.shape("hs", squeeze=True) == (3, 2)

    points = Expanded(lat=0, lon=[1, 2, 3], z=[5, 6])
    points.set_hs(0)
    assert points.size("grid") == (1, 3, 2)
    assert points.size("gridpoint") == ()
    assert points.size("grid", squeeze=True) == (3, 2)
    assert points.size("gridpoint", squeeze=True) == ()
    assert points.shape("hs") == (1, 3, 2)
    assert points.shape("hs", squeeze=True) == (3, 2)


def test_add_g_xz_trivial():
    @add_datavar("hs")
    @add_coord("z", grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(x=0, y=[1, 2, 3], z=5)
    points.set_hs(0)
    assert points.size("grid") == (3, 1, 1)
    assert points.size("gridpoint") == ()
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint", squeeze=True) == ()
    assert points.shape("hs") == (3, 1, 1)
    assert points.shape("hs", squeeze=True) == (3,)

    points = Expanded(lon=0, lat=[1, 2, 3], z=5)
    points.set_hs(0)
    assert points.size("grid") == (3, 1, 1)
    assert points.size("gridpoint") == ()
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint", squeeze=True) == ()
    assert points.shape("hs") == (3, 1, 1)
    assert points.shape("hs", squeeze=True) == (3,)


def test_add_g_yz_trivial():
    @add_datavar("hs")
    @add_coord("z", grid_coord=True)
    class Expanded(GriddedSkeleton):
        pass

    points = Expanded(y=0, x=[1, 2, 3], z=5)
    points.set_hs(0)
    assert points.size("grid") == (1, 3, 1)
    assert points.size("gridpoint") == ()
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint", squeeze=True) == ()
    assert points.shape("hs") == (1, 3, 1)
    assert points.shape("hs", squeeze=True) == (3,)

    points = Expanded(lat=0, lon=[1, 2, 3], z=5)
    points.set_hs(0)
    assert points.size("grid") == (1, 3, 1)
    assert points.size("gridpoint") == ()
    assert points.size("grid", squeeze=True) == (3,)
    assert points.size("gridpoint", squeeze=True) == ()
    assert points.shape("hs") == (1, 3, 1)
    assert points.shape("hs", squeeze=True) == (3,)
