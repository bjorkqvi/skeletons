from geo_skeletons import PointSkeleton, GriddedSkeleton
import pytest
from geo_skeletons.errors import GridError, UnknownVariableError, DirTypeError
import geo_parameters as gp
import numpy as np
def test_from_ds_no_x():
    ds = PointSkeleton(x=(10,29), y=(30,40)).ds()
    ds = ds.rename({'x': 'z'}) 
    ds['z'] = ds.z.drop_attrs # Otherwise 'z' is identified using the standard_name
    with pytest.raises(GridError):
        data = PointSkeleton.from_ds(ds)

def test_from_ds_no_x_no_y():
    ds = PointSkeleton(x=(10,29), y=(30,40)).ds()
    ds = ds.rename({'x': 'z', 'y':'a'}) 
    ds['z'] = ds.z.drop_attrs # Otherwise 'z' is identified using the standard_name
    ds['a'] = ds.a.drop_attrs 
    with pytest.raises(GridError):
        data = PointSkeleton.from_ds(ds)

def test_ind_insert():
    points = PointSkeleton.add_time().add_datavar('hs')(x=(10,29), y=(30,40), time=('2020-01-01 00:00', '2020-01-01 04:00'))
    data = np.array([0,1])
    points.ind_insert('hs',data, time=0)

    points = PointSkeleton.add_time(grid_coord=False).add_datavar('hs', coord_group='grid')(x=(10,29), y=(30,40), time=('2020-01-01 00:00', '2020-01-01 04:00'))
    data = np.array([0,1])
    with pytest.raises(KeyError):
        points.ind_insert('hs',data, time=0)

def test_set_wrong_type():
    points = PointSkeleton.add_datavar('hs')(x=(10,29), y=(30,40))
    points.set('hs', [5,6])
    with pytest.raises(TypeError):
        points.set(5, [5,6])

def test_set_wrong_gp():
    points = PointSkeleton.add_datavar('hs')(x=(10,29), y=(30,40))
    points.set('hs', [5,6])
    with pytest.raises(UnknownVariableError):
        points.set(gp.wave.Tp, [5,6])

def test_set_wrong_dir_type():
    points = PointSkeleton.add_datavar(gp.wave.Dirm)(x=(10,29), y=(30,40))
    points.set('dirm', [5,6])
    with pytest.raises(DirTypeError):
        points.set('dirm', [5,6], dir_type='form')

def test_set_dirtype_for_magnitude():
    points = PointSkeleton.add_datavar('ux').add_datavar('uy').add_magnitude('u', x='ux',y='uy')(x=(10,29), y=(30,40))
    with pytest.raises(DirTypeError):
        points.set('u', [5,6], dir_type='from')

def test_size_wrong_coord_group():
    with pytest.raises(KeyError):
        PointSkeleton(lon=4, lat=5).size('wrong_name')

def test_edges_wrong_type():
    with pytest.raises(KeyError):
        PointSkeleton(lon=4, lat=5).edges('wrong_type')

def test_extent():
    with pytest.raises(KeyError):
        PointSkeleton(lon=4, lat=5).extent('wrong_type')

def test_extent_strict():
    PointSkeleton(lon=4, lat=5).extent('x', strict=True) is None

def test_yank_point_no_pair_given():    
    with pytest.raises(ValueError):
        PointSkeleton(lon=4, lat=5).yank_point()

    with pytest.raises(ValueError):
        PointSkeleton(lon=4, lat=5).yank_point(lon=3)

    with pytest.raises(ValueError):
        PointSkeleton(lon=4, lat=5).yank_point(lat=3)

    with pytest.raises(ValueError):
        PointSkeleton(lon=4, lat=5).yank_point(x=3)

    with pytest.raises(ValueError):
        PointSkeleton(lon=4, lat=5).yank_point(y=3)

    with pytest.raises(ValueError):
        PointSkeleton(lon=4, lat=5).yank_point(lon=3, y=4)

def test_set_name_not_string():
    data = PointSkeleton(lon=0, lat=0)

    with pytest.raises(ValueError):
        data.name = 0
