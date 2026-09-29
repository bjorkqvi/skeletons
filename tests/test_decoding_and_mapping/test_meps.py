import pytest
import xarray as xr
from pathlib import Path
import numpy as np
from geo_skeletons import GriddedSkeleton
# from geo_skeletons.classes import WindGrid

from geo_skeletons.decorators import add_datavar, add_magnitude, add_time
import geo_parameters as gp
@pytest.fixture
def ds():
    """
    Fixture to load the NetCDF dataset for testing.
    Returns an xarray.Dataset object.
    """
    # Get the path to the current file (test_meps.py)
    current_dir = Path(__file__).parent

    # Construct the path to the NetCDF file
    file_path = current_dir / 'meps_wind_only_small_area.nc'

    # Open and return the dataset
    return xr.open_dataset(file_path)


# def test_consistency_components_and_magdir(ds):
#     """Double check that the wind_speed/wind_direction is in lon-lat, and x/y_wind is in projected"""



#     wind_components = WindGrid.add_time().from_ds(ds, priority='components', verbose=True)
#     wind_magdir = WindGrid.add_time().from_ds(ds, priority='magdir', verbose=True)

#     np.testing.assert_array_almost_equal(ds.wind_speed.values.squeeze(), wind_magdir.ff(), decimal=3)
#     np.testing.assert_array_almost_equal(ds.wind_speed.values.squeeze(), wind_components.ff(), decimal=3)
#     ddiff = np.abs(np.mod(ds.wind_direction.values.squeeze(), 360) - wind_magdir.dd())
#     ddiff = np.minimum(ddiff, 360-ddiff)

#     np.testing.assert_array_almost_equal(ddiff, 0, decimal=4)

#     np.testing.assert_array_almost_equal(wind_components.ff(), wind_magdir.ff(), decimal=3)


#     np.testing.assert_array_almost_equal(wind_components.u(), wind_magdir.u(), decimal=2)
#     np.testing.assert_array_almost_equal(wind_components.v(), wind_magdir.v(), decimal=2)  

#     breakpoint()

def test_check_consistency_with_northwind_ywind(ds):
    from geo_skeletons.classes import WindGrid
    @add_magnitude(gp.wind.Wind)
    @add_datavar(gp.wind.NorthWind('v'))
    @add_datavar(gp.wind.EastWind('u'))
    @add_time()
    class WindLonLat(GriddedSkeleton):
        pass


    wind_components = WindLonLat.from_ds(ds, priority='components', verbose=True)
    #wind_magdir = WindLonLat.from_ds(ds, priority='magdir', verbose=True)
    #wind_magdir.v(projected=True)[0,0,0]
    breakpoint()
    #wind_magdir.set(gp.wind.XWind, ds.x_wind_10m.values)
    