from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_datavar
import numpy as np
import geo_parameters as gp
import pytest
from geo_skeletons.errors import DirTypeError


def test_set_method_no_gp():
    @add_datavar("wdir", dir_type="from")
    class WaveDir(GriddedSkeleton):
        pass

    grid = WaveDir(lon=(0, 3), lat=(60, 70))
    grid.set_wdir(0)
    orig_dir = grid.wdir()
    grid.set_wdir(180, dir_type="to")
    np.testing.assert_array_almost_equal(orig_dir, grid.wdir())
    grid.set_wdir(-np.pi / 2, dir_type="math")
    np.testing.assert_array_almost_equal(orig_dir, grid.wdir())
    grid.set_wdir(2 * np.pi - np.pi / 2, dir_type="math")
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
    grid.set("wdir", -np.pi / 2, dir_type="math")
    np.testing.assert_array_almost_equal(orig_dir, grid.wdir())
    grid.wdir("wdir", 2 * np.pi - np.pi / 2, dir_type="math")
    np.testing.assert_array_almost_equal(orig_dir, grid.wdir())


def test_set_method():
    @add_datavar(gp.wave.Dirp)  # From direction
    class WaveDir(GriddedSkeleton):
        pass

    grid = WaveDir(lon=(0, 3), lat=(60, 70))
    grid.set_dirp(0)
    orig_dir = grid.dirp()
    grid.set_dirp(180, dir_type="to")
    np.testing.assert_array_almost_equal(orig_dir, grid.dirp())
    grid.set_dirp(-np.pi / 2, dir_type="math")
    np.testing.assert_array_almost_equal(orig_dir, grid.dirp())


def test_general_set_method():
    @add_datavar(gp.wave.Dirp)  # From direction
    class WaveDir(GriddedSkeleton):
        pass

    grid = WaveDir(lon=(0, 3), lat=(60, 70))
    grid.set_dirp(0)
    orig_dir = grid.dirp()
    grid.set("dirp", 180, dir_type="to")
    np.testing.assert_array_almost_equal(orig_dir, grid.dirp())
    grid.set("dirp", -np.pi / 2, dir_type="math")
    np.testing.assert_array_almost_equal(orig_dir, grid.dirp())


def test_setting_dir_type_for_scalar_raises_error():
    @add_datavar("wdir")
    class WaveDir(GriddedSkeleton):
        pass

    grid = WaveDir(lon=(0, 3), lat=(60, 70))
    grid.set_wdir(0)
    orig_dir = grid.wdir()
    with pytest.raises(DirTypeError):
        grid.set("wdir", 180, dir_type="to")
