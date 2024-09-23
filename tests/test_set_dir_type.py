from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_datavar
import numpy as np
import geo_parameters as gp


def test_set_method_no_gp():
    @add_datavar("wdir", dir_type="from")
    class WaveDir(GriddedSkeleton):
        pass

    grid = WaveDir(lon=(0, 3), lat=(60, 70))
    grid.set_wdir(0)
    orig_dir = grid.wdir()
    grid.set_wdir(180, dir_type="to")
    np.testing.assert_array_almost_equal(orig_dir, grid.wdir())


def test_general_set_method_no_gp():
    @add_datavar("wdir", dir_type="from")
    class WaveDir(GriddedSkeleton):
        pass

    grid = WaveDir(lon=(0, 3), lat=(60, 70))
    grid.set_wdir(0)
    orig_dir = grid.wdir()
    grid.set("wdir", 180, dir_type="to")
    np.testing.assert_array_almost_equal(orig_dir, grid.wdir())


def test_set_method():
    @add_datavar(gp.wave.Dirp)
    class WaveDir(GriddedSkeleton):
        pass

    grid = WaveDir(lon=(0, 3), lat=(60, 70))
    grid.set_wdir(0)
    orig_dir = grid.wdir()
    grid.set_wdir(180, dir_type="to")
    np.testing.assert_array_almost_equal(orig_dir, grid.wdir())


def test_general_set_method():
    @add_datavar(gp.wave.Dirp)  # From direction
    class WaveDir(GriddedSkeleton):
        pass

    grid = WaveDir(lon=(0, 3), lat=(60, 70))
    grid.set_wdir(0)
    orig_dir = grid.wdir()
    grid.set("wdir", 180, dir_type="to")
    np.testing.assert_array_almost_equal(orig_dir, grid.wdir())
