import pytest
import xarray as xr
from pathlib import Path
import numpy as np
from geo_skeletons import GriddedSkeleton
from geo_skeletons.classes import WindGrid

from geo_skeletons.decorators import add_datavar, add_magnitude, add_time
import geo_parameters as gp
import time
@pytest.fixture
def ds():
    """
    Fixture to load the NetCDF dataset for testing.
    Returns an xarray.Dataset object.
    """
    # Get the path to the current file (test_meps.py)
    current_dir = Path(__file__).parent

    # Construct the path to the NetCDF file
    file_path = current_dir / 'meps_small_test.nc'

    # Open and return the dataset
    return xr.open_dataset(file_path)

def test_check_consistency_decode_components(ds):
    @add_magnitude(gp.wind.Wind)
    @add_datavar(gp.wind.NorthWind('v'))
    @add_datavar(gp.wind.EastWind('u'))
    @add_time()
    class WindLonLat(GriddedSkeleton):
        pass


    wind_components = WindLonLat.from_ds(ds, priority='components')
    np.testing.assert_array_almost_equal(ds.x_wind_10m.values.squeeze(), wind_components.u(wrt='crs'), decimal=3)
    np.testing.assert_array_almost_equal(ds.y_wind_10m.values.squeeze(), wind_components.v(wrt='crs'), decimal=3)
    np.testing.assert_array_almost_equal(ds.wind_speed.values.squeeze(), wind_components.ff(), decimal=3)
    
    # Only check one point, since there are apparently some differences in how to reprojection was made, leading to some values with a couple of degrees difference
    # We are not checking the reprojecton here, but to see that everything was decoded correctly
    diff = (ds.wind_direction.values.squeeze()[0,10,:] - wind_components.dd()[0,10,:])
    diff = np.mod(diff, 360)
    diff = np.minimum(diff, 360-diff)
    np.testing.assert_array_almost_equal(diff, 0, decimal=1)

def test_check_consistency_decode_magdir(ds):
    
    @add_magnitude(gp.wind.Wind)
    @add_datavar(gp.wind.NorthWind('v'))
    @add_datavar(gp.wind.EastWind('u'))
    @add_time()
    class WindLonLat(GriddedSkeleton):
        pass


    wind_components = WindLonLat.from_ds(ds, priority='magdir')
    np.testing.assert_array_almost_equal(ds.x_wind_10m.values.squeeze(), wind_components.u(wrt='crs'), decimal=2)
    np.testing.assert_array_almost_equal(ds.y_wind_10m.values.squeeze(), wind_components.v(wrt='crs'), decimal=2)
    np.testing.assert_array_almost_equal(ds.wind_speed.values.squeeze(), wind_components.ff(), decimal=2)
    
    # Only check one point, since there are apparently some differences in how to reprojection was made, leading to some values with a couple of degrees difference
    # We are not checking the reprojecton here, but to see that everything was decoded correctly
    diff = (ds.wind_direction.values.squeeze()[0,10,:] - wind_components.dd()[0,10,:])
    diff = np.mod(diff, 360)
    diff = np.minimum(diff, 360-diff)
    np.testing.assert_array_almost_equal(diff, 0, decimal=1)

def test_crs_components(ds):
    wind = WindGrid.add_time().from_ds(ds, priority='components')
    np.testing.assert_array_almost_equal(ds.x_wind_10m.values.squeeze(), wind.u(), decimal=2)
    np.testing.assert_array_almost_equal(ds.y_wind_10m.values.squeeze(), wind.v(), decimal=2)
    np.testing.assert_array_almost_equal(ds.wind_speed.values.squeeze(), wind.ff(), decimal=2)
    
    # Only check one point, since there are apparently some differences in how to reprojection was made, leading to some values with a couple of degrees difference
    # We are not checking the reprojecton here, but to see that everything was decoded correctly
    diff = (ds.wind_direction.values.squeeze()[0,10,:] - wind.dd()[0,10,:])
    diff = np.mod(diff, 360)
    diff = np.minimum(diff, 360-diff)
    np.testing.assert_array_almost_equal(diff, 0, decimal=1)

def test_time_for_calculating_dd_with_rotation(ds):
    # Start the timer for the entire test
    start_total = time.time()

    # Step 1: Add time to WindGrid and create the wind object
    start = time.time()
    wind = WindGrid.add_time().from_ds(ds, priority='components')
    print(f"Step 1 (Create WindGrid): {time.time() - start:.6f} seconds")

    start = time.time()
    dd = wind.dd()
        
    start = time.time()
    dd = wind.dd()
    print(f"Step 3 (Calculate dd - second time): {time.time() - start:.6f} seconds")

    start = time.time()
    dd = wind.dd()
    print(f"Step 4 (Calculate dd - third time): {time.time() - start:.6f} seconds")

def test_yank_point_speed(ds):
    @add_magnitude(gp.wind.Wind)
    @add_datavar(gp.wind.NorthWind('v'))
    @add_datavar(gp.wind.EastWind('u'))
    @add_time()
    class WindLonLat(GriddedSkeleton):
        pass

    x0, y0 = -810084.06,-1082517.9

    start = time.time()
    wind = WindGrid.add_time().from_ds(ds, priority='components').sel(x=x0, y=y0)
    wind.dd()
    print(f"Time to sel point and calculate dd: {time.time() - start:.6f} seconds")

    start = time.time()
    grid = GriddedSkeleton.from_ds(ds)
    inds = grid.yank_point(x=x0, y=y0)
    wind2 = WindGrid.add_time().from_ds(ds.isel(x=inds['inds_x'][0], y=inds['inds_y'][0]), priority='components')
    wind2.dd()
    print(f"Time to yank point and calculate dd: {time.time() - start:.6f} seconds")

def test_yank_point_speed_lonlat(ds):
    
    
    @add_magnitude(gp.wind.Wind)
    @add_datavar(gp.wind.NorthWind('v'))
    @add_datavar(gp.wind.EastWind('u'))
    @add_time()
    class WindLonLat(GriddedSkeleton):
        pass

    lon0, lat0 =3.02166538, 52.93508008

    start = time.time()
    wind = WindGrid.add_time().from_ds(ds, priority='magdir').sel(lon=lon0, lat=lat0)
    wind.dd()
    print(f"Time to sel point and calculate dd: {time.time() - start:.6f} seconds")

    start = time.time()
    grid = GriddedSkeleton.from_ds(ds)
    inds = grid.yank_point(lon=lon0, lat=lat0)
    dds = ds.isel(x=inds['inds_x'][0], y=inds['inds_y'][0])
    wind2 = WindGrid.add_time().from_ds(dds, priority='magdir')
    wind2.dd()
    print(f"Time to yank point and calculate dd: {time.time() - start:.6f} seconds")

    start = time.time()
    wind3 = WindGrid.add_time().from_ds(ds, priority='magdir').ravel()
    wind3 = wind3.sel(lon=lon0, lat=lat0)
    wind3.dd()
    print(f"Time to sel point and calculate dd: {time.time() - start:.6f} seconds")



