import xarray as xr
def set_core_vars_to_skeleton_from_ds(skeleton, ds: xr. Dataset, core_vars: dict, coord_map: dict, meta_dict: dict=None,data_vars: list[str] = None):
    """Set core (static) variables to a skeleton from an xarray Dataset.
    
    Using the function 'geo_skeleton.decorders.identify_core_in_ds' we have gotten
    core_vars: dict mapping core variable to ds variable name
    coord_map: dict mapping variables of a core var ['time','inds','freq'] to variables of a ds var ['time','x','frequency']
    
    Optional:
    data_vars [default None]: list of ds_variables that will be set. All set if None.
    meta_dict: dict of core-var specific meta-data"""

    data_vars = data_vars or []
    meta_dict = meta_dict or {}
    for var, ds_var in core_vars.items():
        if not data_vars or ds_var in data_vars: # If list is specified, only add those variables 
        
            skeleton.set(var, ds.get(ds_var), coords=coord_map[var])
            metadata = meta_dict.get(var) or ds.get(ds_var).attrs
            skeleton.meta.append(metadata, name=var)

    for var in skeleton.core.magnitudes():
        metadata = meta_dict.get(var)
        skeleton.meta.append(metadata, var)

    for var in skeleton.core.directions():
        metadata = meta_dict.get(var)
        skeleton.meta.append(metadata, var)

    return skeleton