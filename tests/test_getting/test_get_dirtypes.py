import geo_parameters as gp
from geo_skeletons import PointSkeleton, GriddedSkeleton
from geo_skeletons.decorators import add_datavar
import pytest 
import numpy as np

@pytest.fixture
def wave():
    @add_datavar(gp.wave.Dirp)
    class Wave(PointSkeleton):
        pass

    data = Wave(lon=range(10), lat=range(10))
    data.set_dirp(180)
    
    return data

@pytest.fixture
def wavegrid():
    @add_datavar(gp.wave.Dirp)
    class Wave(GriddedSkeleton):
        pass

    data = Wave(lon=range(10), lat=range(10))
    data.set_dirp(180)
    
    return data

@pytest.fixture
def waveto():
    @add_datavar(gp.wave.DirpTo)
    class Wave(PointSkeleton):
        pass

    data = Wave(lon=range(10), lat=range(10))
    data.set_dirp(0)
    
    return data

@pytest.fixture
def wavegridto():
    @add_datavar(gp.wave.DirpTo)
    class Wave(GriddedSkeleton):
        pass

    data = Wave(lon=range(10), lat=range(10))
    data.set_dirp(0)
    
    return data

def test_get_dir_type_point(wave):
    np.testing.assert_array_almost_equal(wave.dirp(),180)
    np.testing.assert_array_almost_equal(wave.dirp(dir_type='from'),180)
    np.testing.assert_array_almost_equal(wave.dirp(dir_type='to'),0)

def test_get_dir_type_point_gp(wave):
    np.testing.assert_array_almost_equal(wave.get(gp.wave.Dirp),180)
    np.testing.assert_array_almost_equal(wave.get(gp.wave.Dirp, dir_type='from'),180)
    np.testing.assert_array_almost_equal(wave.get(gp.wave.Dirp, dir_type='to'),0)

def test_get_dir_type_point_gp_opposite(wave):
    np.testing.assert_array_almost_equal(wave.get(gp.wave.DirpTo),0)
    np.testing.assert_array_almost_equal(wave.get(gp.wave.DirpTo, dir_type='from'),180)
    np.testing.assert_array_almost_equal(wave.get(gp.wave.DirpTo, dir_type='to'),0)

def test_get_dir_type_grid(wavegrid):
    np.testing.assert_array_almost_equal(wavegrid.dirp(),180)
    np.testing.assert_array_almost_equal(wavegrid.dirp(dir_type='from'),180)
    np.testing.assert_array_almost_equal(wavegrid.dirp(dir_type='to'),0)

def test_get_dir_type_grid_gp(wavegrid):
    np.testing.assert_array_almost_equal(wavegrid.get(gp.wave.Dirp),180)
    np.testing.assert_array_almost_equal(wavegrid.get(gp.wave.Dirp, dir_type='from'),180)
    np.testing.assert_array_almost_equal(wavegrid.get(gp.wave.Dirp, dir_type='to'),0)

def test_get_dir_type_grid_gp_opposite(wavegrid):
    np.testing.assert_array_almost_equal(wavegrid.get(gp.wave.DirpTo),0)
    np.testing.assert_array_almost_equal(wavegrid.get(gp.wave.DirpTo, dir_type='from'),180)
    np.testing.assert_array_almost_equal(wavegrid.get(gp.wave.DirpTo, dir_type='to'),0)



def test_get_dir_type_point_to(waveto):
    np.testing.assert_array_almost_equal(waveto.dirp(),0)
    np.testing.assert_array_almost_equal(waveto.dirp(dir_type='from'),180)
    np.testing.assert_array_almost_equal(waveto.dirp(dir_type='to'),0)

def test_get_dir_type_point_gp_to(waveto):
    np.testing.assert_array_almost_equal(waveto.get(gp.wave.Dirp),180)
    np.testing.assert_array_almost_equal(waveto.get(gp.wave.Dirp, dir_type='from'),180)
    np.testing.assert_array_almost_equal(waveto.get(gp.wave.Dirp, dir_type='to'),0)

def test_get_dir_type_point_gp_opposite_to(waveto):
    np.testing.assert_array_almost_equal(waveto.get(gp.wave.DirpTo),0)
    np.testing.assert_array_almost_equal(waveto.get(gp.wave.DirpTo, dir_type='from'),180)
    np.testing.assert_array_almost_equal(waveto.get(gp.wave.DirpTo, dir_type='to'),0)

def test_get_dir_type_grid_to(wavegridto):
    np.testing.assert_array_almost_equal(wavegridto.dirp(),0)
    np.testing.assert_array_almost_equal(wavegridto.dirp(dir_type='from'),180)
    np.testing.assert_array_almost_equal(wavegridto.dirp(dir_type='to'),0)

def test_get_dir_type_grid_gp_to(wavegridto):
    np.testing.assert_array_almost_equal(wavegridto.get(gp.wave.Dirp),180)
    np.testing.assert_array_almost_equal(wavegridto.get(gp.wave.Dirp, dir_type='from'),180)
    np.testing.assert_array_almost_equal(wavegridto.get(gp.wave.Dirp, dir_type='to'),0)

def test_get_dir_type_grid_gp_opposite_to(wavegridto):
    np.testing.assert_array_almost_equal(wavegridto.get(gp.wave.DirpTo),0)
    np.testing.assert_array_almost_equal(wavegridto.get(gp.wave.DirpTo, dir_type='from'),180)
    np.testing.assert_array_almost_equal(wavegridto.get(gp.wave.DirpTo, dir_type='to'),0)
