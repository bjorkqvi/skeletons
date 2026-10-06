from geo_skeletons.classes import WindGrid
from geo_parameters.wind import XWind
import geo_parameters as gp
from geo_parameters.wind import EastWind
from geo_skeletons import GriddedSkeleton
def test_compile_ds():
    data = WindGrid(lon=(10,20), lat=(50,60))
    data.set_u(2)
    data.set_v(3)
    
    
    ds = data.ds()
    assert set(ds.data_vars) ==  {'crs', 'u', 'v'}

    ds = data.ds(compile=True)
    assert set(ds.data_vars) ==  {'crs', 'u', 'v', 'ff','dd'}

    da = data.u(wrt=data.proj.utm(), data_array=True)
    meta_dict = XWind.meta_dict()
    meta_dict['grid_mapping'] = 'crs'
    assert da.attrs == meta_dict



def test_compile_ds_xy():
    data = WindGrid(x=(571666.44750366,1111654.03336625), y=(5539109.81566836,6702495.15549543), crs=(32, 'U'))
    data.set_u(2)
    data.set_v(3)

    
    ds = data.ds()
    assert set(ds.data_vars) ==  {'crs', 'u', 'v'}

    ds = data.ds(compile=True)
    assert set(ds.data_vars) ==  {'crs', 'u', 'v', 'ff','dd'}

    da = data.u(wrt=data.proj.utm(), data_array=True)
    meta_dict = XWind.meta_dict()
    meta_dict['grid_mapping'] = 'crs'
    assert da.attrs == meta_dict


def test_compile_ds_east_north_components():
    Wind = GriddedSkeleton.add_datavar([gp.wind.EastWind('u'), gp.wind.NorthWind('v')]).add_magnitude(gp.wind.Wind)

    data = Wind(x=(571666.44750366,1111654.03336625), y=(5539109.81566836,6702495.15549543), crs=(32, 'U'))

    data.set_u(2)
    data.set_v(3)

    
    ds = data.ds()
    assert set(ds.data_vars) ==  {'crs', 'u', 'v'}

    ds = data.ds(compile=True)
    assert set(ds.data_vars) ==  {'crs', 'u', 'v', 'ff','dd'}

    da = data.u(wrt=data.proj.utm(), data_array=True)
    meta_dict = XWind.meta_dict()
    meta_dict['grid_mapping'] = 'crs'
    assert da.attrs == meta_dict

def test_compile_ds_xy_unrotate():
    data = WindGrid(x=(571666.44750366,1111654.03336625), y=(5539109.81566836,6702495.15549543), crs=(32, 'U'))
    data.set_u(2)
    data.set_v(3)

    
    ds = data.ds()
    assert set(ds.data_vars) ==  {'crs', 'u', 'v'}

    ds = data.ds(compile=True)
    assert set(ds.data_vars) ==  {'crs', 'u', 'v', 'ff','dd'}

    da = data.u(wrt='lonlat', data_array=True)
    meta_dict = EastWind.meta_dict()
    meta_dict['grid_mapping'] = 'crs'
    assert da.attrs == meta_dict
