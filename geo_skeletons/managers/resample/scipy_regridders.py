from scipy.interpolate import griddata
import numpy as np
from geo_skeletons.errors import GridError


def scipy_gridded_to_gridded(data, new_grid, new_data, method: str ='nearest'):
    """Uses a simple scipy griddata to regrid gridded data to gridded data.
    
    Can only interpolate spatial data for now (not time variable allowed)."""
    if not new_grid.is_gridded():
        raise GridError('Can only handel gridded data for now!')
    if not data.is_gridded():
        raise GridError('Can only handel gridded data for now!')
    



    # Determine the coordinates
    target_lon, target_lat = new_grid.longrid(native=True), new_grid.latgrid(native=True)
    if new_data.core.is_cartesian():
        lon, lat = data.xy()
    else:
        lon, lat = data.lonlat()
    points = np.array([(lon, lat) for lon, lat in zip(lon, lat)])


    spatial_coords = data.core.coords('spatial')
    print(f'Original data has spatial coords {spatial_coords}')
    if new_data.core.is_cartesian():
        print(f"Target grid has spatial coords {new_data.core.coords('spatial')} UTM {new_data.utm.zone()}")
    else:
        print(f"Target grid has spatial coords {new_data.core.coords('spatial')}")

    for var_name in data.core.data_vars('all'):
        var = data.core.get(var_name)
        var_coords = data.core.coords(var.coord_group)
        if var_coords == spatial_coords:
            print(f"'{var_name}' {var_coords}: Regridding...")
            new_array = griddata(points, data.get(var_name).flatten(), (target_lon, target_lat), method=method)
            new_data.set(var_name, new_array)
        elif set(spatial_coords + ['time']) == set(var_coords):
            print(f"'{var_name}' {var_coords}: Regridding over time.", end='')
            new_array = np.empty(new_data.shape(var_name))
            Nt = len(data.time())
            ct = 0
            for t in range(Nt):
                if ct > Nt/10:
                    print('.', end='')
                    ct = 0

                # Flatten the data for the current time step
                source_values = data.get(var_name)[t, :, :].flatten()

                # Interpolate to the target grid
                new_array[t, :, :] = griddata(points, source_values, (target_lon, target_lat), method=method)
                ct += 1
            new_data.set(var_name, new_array)
            print('')
        else:
            print(f"'{var_name}' {var_coords}: Skipping!")
        

    return new_data


scipy_regridders = {'gridded_to_gridded': scipy_gridded_to_gridded}