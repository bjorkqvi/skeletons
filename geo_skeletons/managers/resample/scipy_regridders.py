from scipy.interpolate import griddata, RegularGridInterpolator
import numpy as np
from geo_skeletons.errors import GridError, SkeletonError
from copy import copy
from geo_skeletons.dask_computations import undask_me

def data_doesnt_cover_request(x,y, xq,yq):
    return min(xq) < min(x) or max(xq) > max(x) or min(yq) < min(y) or max(yq) > max(y)

def data_close_to_wrapping_180(lon, dlon):
    if 180-lon[-1] > 5*dlon:
        return False
    if lon[0]-(-180) > 5*dlon:
        return False
    return True

def scipy_regrid_gridded_data(data, new_grid, new_data, verbose, method: str='nearest', drop_nan: bool=False, mask_nan: float=None,**kwargs):
    """Regrids gridded data using RegularGridInterpolator, which is faster than griddata"""
    
    if not data.is_gridded():
        raise Exception('Input data needs to be gridded!')
    
    if verbose:
        print(f"Using method '{method}'")

    if drop_nan:
        print(f'Cannot drop nans and keep data gridded. Redirecting interpolation to {scipy_regrid_point_data}')
        new_data = scipy_regrid_point_data(data, new_grid, new_data, verbose, method=method, drop_nan=drop_nan, mask_nan=mask_nan,**kwargs)
        return new_data

    # Needs to use native here since it has to be regularly gridded
    x, y = data.lon(native=True), data.lat(native=True)
    # Pad the longitude edges to have values if we are wrapping around +-180
    
    if 'time' in data.core.coords():
        t = data.time()
        t = np.array((t-t[0]).total_seconds()).astype(int)

    spatial_coords = data.core.coords('spatial')
    
    if new_data.core.x_str == 'lon' and data.core.x_str == 'lon' and new_data.is_gridded(): # Go from gridded to gridded
        xq, yq = new_data.lon(native=True), new_data.lat(native=True)
        
        if 'time' in data.core.coords():
            T, Y, X = np.meshgrid(t, yq, xq, indexing="ij")  # Use "ij" indexing for (t, y, x) order
            query_points_time = np.column_stack([T.ravel(), Y.ravel(), X.ravel()])
        
        Y, X = np.meshgrid(yq, xq, indexing="ij")  # Use "ij" indexing for (t, y, x) order
        query_points = np.column_stack([Y.ravel(), X.ravel()])  
    else: # We need to get the query points in the native coordinates of the original data, which will make is non-gridded
        if data.core.is_projected():
            xq, yq = new_data.xy(crs=data.proj.crs())
        else:
            xq, yq = new_data.lonlat()
        
        query_points = np.column_stack((yq, xq))

        if 'time' in data.core.coords():
            t_repeated = np.repeat(t, len(xq)) 
            xq_tiled = np.tile(xq, len(t))  
            yq_tiled = np.tile(yq, len(t)) 

            query_points_time = np.column_stack((t_repeated, yq_tiled, xq_tiled))
        
            
    # Check that we are not out of bounds, since RegularGridInterpolator can't handle that
    # We also can't drop nan values and still keep data gridded

    x_expanded = False
    if data_doesnt_cover_request(x,y,xq,yq) and not data.core.is_projected():
        if data_close_to_wrapping_180(x, data.dlon()):
            print("Data doesn't cover requested area. Trying to see if it is a +-180 longitude wrapping issue...")
            x = np.concatenate(([x[-1]-360], x, [x[0]+360]))
            x_expanded = True
    
    if data_doesnt_cover_request(x,y,xq,yq):
        x, y = data.lon(native=True), data.lat(native=True)
        print(f"Data ({data.core.x_str}={data.edges(data.core.x_str)}, {data.core.y_str}={data.edges(data.core.y_str)}) doesnt cover new grid ({data.core.x_str}=({min(xq):.4f}, {max(xq):.4f}, {data.core.y_str}=({min(yq):.4f}, {max(yq):.4f})). Redirecting interpolation to {scipy_regrid_point_data}")
        new_data = scipy_regrid_point_data(data, new_grid, new_data, verbose, method=method, drop_nan=drop_nan, mask_nan=mask_nan,**kwargs)
        return new_data

    if verbose:
        if mask_nan is not None:
            print(f"Replacing nan values with {mask_nan}")


    for var_name in data.core.data_vars('all'):
            if data.get(var_name, strict=True) is not None:
                var = data.core.get(var_name)
                var_coords = data.core.coords(var.coord_group)
                squeezed_var_coords = data.coord_squeeze(data.core.coords(var.coord_group))
                time_regridding = False
                if set(var_coords) != set(squeezed_var_coords):
                    if verbose:
                        print(f"Ignoring coordinates {list(set(var_coords)-set(squeezed_var_coords))} with trivial dimensions...")
                if var_coords == spatial_coords:
                    if verbose:
                        print(f"'{var_name}' {var_coords}: Regridding...")
                    target_points = (y, x)
                    qp = query_points
                elif set(spatial_coords + ['time']) == set(squeezed_var_coords) and 'time' in new_data.core.coords():
                    time_regridding = True
                    if verbose:
                        print(f"'{var_name}' {var_coords}: Regridding over time...")
                    target_points = (t, y, x)
                    qp = query_points_time
                else:
                    if verbose:
                        print(f"'{var_name}' {var_coords}: Skipping!")
                    continue
                
                source_values = data.get(var_name)
                if x_expanded:
                    print('Fixed +-180 longitude wrapping issue and expanding data to match...')

                    if time_regridding:
                        first_column = source_values[:, :,0] 
                        last_column = source_values[:,:,-1] 
                        ax = 2
                    else:
                        first_column = source_values[:,0] 
                        last_column = source_values[:, -1] 
                        ax = 1
                    source_values = np.concatenate([np.expand_dims(last_column, axis=-1), source_values, np.expand_dims(first_column, axis=-1)], axis=ax) 

                if mask_nan is not None:
                    source_values = copy(source_values)
                    mask = np.isnan(source_values)
                    source_values[mask] = mask_nan
                interpolator = RegularGridInterpolator(target_points, undask_me(source_values), method=method)
                interpolated_values = interpolator(qp)
                
                new_data.set(var_name, interpolated_values, fit_to_data=True)
                
    return new_data



def scipy_regrid_point_data(data, new_grid, new_data, verbose, method: str ='nearest', drop_nan: bool=False, mask_nan: float=None,**kwargs):
    """Uses a simple scipy griddata to regrid non-gridded data."""
    if verbose:
        print(f"Using method '{method}'")
    # Determine the coordinates
    if new_grid.is_gridded():
        target_lon, target_lat = new_grid.longrid(native=True), new_grid.latgrid(native=True)
    else:
        target_lon, target_lat = new_grid.lonlat(native=True)
    if new_data.core.is_projected():
        lon, lat = data.xy(crs=new_data.proj.crs())
    else:
        lon, lat = data.lonlat()
        if lon is None:
            raise SkeletonError("Can't resample a cartesian grid without a projection to a spherical grid!")
    all_points = np.array([(lon, lat) for lon, lat in zip(lon, lat)])


    spatial_coords = data.core.coords('spatial')

    if verbose:
        if drop_nan:
            print("Excluding nan values from interpolation")
        elif mask_nan is not None:
            print(f"Replacing nan values with {mask_nan}")

    for var_name in data.core.data_vars('all'):
        if var_name not in ['x','y','lon','lat']:
            if data.get(var_name, strict=True) is not None:
                var = data.core.get(var_name)
                var_coords = data.core.coords(var.coord_group)
                squeezed_var_coords = data.coord_squeeze(data.core.coords(var.coord_group))
                if set(var_coords) != set(squeezed_var_coords):
                    if verbose:
                        print(f"Ignoring coordinates {list(set(var_coords)-set(squeezed_var_coords))} with trivial dimensions...")
                if var_coords == spatial_coords:
                    if verbose:
                        print(f"'{var_name}' {var_coords}: Regridding...")
                    source_values = data.get(var_name).flatten()
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
                    
                    
                    new_array = griddata(points, source_values, (target_lon, target_lat), method=method)
                    new_data.set(var_name, new_array)
                elif set(spatial_coords + ['time']) == set(squeezed_var_coords) and 'time' in new_data.core.coords():
                    if verbose:
                        print(f"'{var_name}' {var_coords}: Regridding over time.", end='')
                    new_array = np.empty(new_data.shape(var_name))
                    Nt = len(data.time())
                    ct = 0
                    for t in range(Nt):
                        if ct > Nt/10:
                            if verbose:
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

scipy_regridders = {'gridded_to_gridded': scipy_regrid_gridded_data,'point_to_gridded': scipy_regrid_point_data, 'gridded_to_point': scipy_regrid_gridded_data, 'point_to_point': scipy_regrid_point_data,'available': True, 'installation': 'Native (default)', 'options': 'drop_nan: bool, mask_nan: float'}