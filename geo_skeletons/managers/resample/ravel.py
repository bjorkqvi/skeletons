def ravel_regrid(data, new_grid, new_data, verbose, **kwargs):
    """Regrids the data by ravelling. Actually just used to get a GriddedSkeleton to a PointSkeleton that contains the same data
    Can also be used to go from a spherical PointSkeleton to a cartesian PointSkeleton or vice versa"""
    for var in data.core.data_vars():
        if data.get(var) is None:
            continue
        if not data.is_gridded():
            new_data.set(var, data.get(var))
        else:
            shape = data.shape(var)
            if 'time' in data.core.coords(data.core.coord_group(var)):
                new_data.set(var,data.get(var).reshape(shape[0],-1, *shape[3:]))
            else:
                new_data.set(var,data.get(var).reshape(-1, *shape[2:]))
    
    if not new_data.core.is_projected():
        new_data.proj.set_utm(data.proj.utm())
    return new_data
ravel_regridders = {'gridded_to_point': ravel_regrid, 'point_to_point': ravel_regrid,'available': True, 'installation': 'Native', 'options': "N/A"}