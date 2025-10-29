from scipy.interpolate import griddata
import numpy as np
from geo_skeletons.errors import GridError


def scipy_griddata(data, new_grid, new_data, verbose, method: str ='nearest', drop_nan: bool=False, mask_nan: float=None,**kwargs):
    """Uses a simple scipy griddata to regrid gridded data to gridded data.
    
    Can only interpolate spatial data for now (not time variable allowed)."""

    # Determine the coordinates
    if new_grid.is_gridded():
        target_lon, target_lat = new_grid.longrid(native=True), new_grid.latgrid(native=True)
    else:
        target_lon, target_lat = new_grid.lonlat(native=True)
    if new_data.core.is_cartesian():
        lon, lat = data.xy()
    else:
        lon, lat = data.lonlat()
    all_points = np.array([(lon, lat) for lon, lat in zip(lon, lat)])


    spatial_coords = data.core.coords('spatial')

    if verbose:
        if drop_nan:
            print("Excluding nan values from interpolation")
        elif mask_nan is not None:
            print(f"Replacing nan values with {mask_nan}")

    for var_name in data.core.data_vars('all'):
        if var_name not in ['x','y','lon','lat']:
            var = data.core.get(var_name)
            var_coords = data.core.coords(var.coord_group)
            if var_coords == spatial_coords:
                if verbose:
                    print(f"'{var_name}' {var_coords}: Regridding...")
                new_array = griddata(points, data.get(var_name).flatten(), (target_lon, target_lat), method=method)
                new_data.set(var_name, new_array)
            elif set(spatial_coords + ['time']) == set(var_coords):
                if verbose:
                    print(f"'{var_name}' {var_coords}: Regridding over time.", end='')
                new_array = np.empty(new_data.shape(var_name))
                Nt = len(data.time())
                ct = 0
                for t in range(Nt):
                    if ct > Nt/10:
                        print('.', end='')
                        ct = 0

                    # Flatten the data for the current time step
                    source_values = data.get(var_name)[t, ...].flatten()
                    if drop_nan:
                        mask = np.logical_not(np.isnan(source_values))
                        source_values = source_values[mask]
                        
                        points = all_points[mask]
                    elif mask_nan is not None:
                        mask = np.isnan(source_values)
                        source_values[mask] = mask_nan
                        points = all_points
                    else:
                        points = all_points
                    # Interpolate to the target grid
                    new_array[t, ...] = griddata(points, source_values, (target_lon, target_lat), method=method)
                    ct += 1
                new_data.set(var_name, new_array)
                if verbose:
                    print('')
            else:
                if verbose:
                    print(f"'{var_name}' {var_coords}: Skipping!")
            

    return new_data

scipy_regridders = {'gridded_to_gridded': scipy_griddata,'point_to_gridded': scipy_griddata, 'gridded_to_point': scipy_griddata, 'point_to_point': scipy_griddata,'available': True, 'installation': 'Native (default)', 'options': 'drop_nan: bool, mask_nan: float'}