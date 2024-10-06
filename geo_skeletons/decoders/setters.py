import xarray as xr
from .decoders import map_ds_to_gp, _remap_coords
import geo_parameters as gp
def set_core_vars_to_skeleton_from_ds(skeleton, ds: xr.Dataset, core_vars: dict, coord_map: dict, meta_dict: dict=None,data_vars: list[str] = None):
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


def add_dynamic_vars_from_ds(skeleton, ds:xr.Dataset, core_coords:dict, core_aliases:dict,keep_ds_names:bool,aliases: dict, data_vars: list[str] = None):
    """Find all the variables in the Dataset that can e added to the Skeleton.
    
    Reasons not to add: 
    1) It already exists (based on name or standard_name of geo-parameter)
    2) We can't find a coordinate groupt that fits the dimensions"""
    settable_vars = {}
    core_vars = {}
    coord_map = {}    
    data_vars = data_vars or []
    mapped_vars, __ = map_ds_to_gp(ds, keep_ds_names=keep_ds_names, aliases=aliases)
    for ds_var, var in mapped_vars.items():
        # 1) Check if variable exists
        var_str, __ = gp.decode(var)
        if __ is not None:
            var_exists = skeleton.core.find_cf(var.standard_name()) != [] or var_str in skeleton.core.data_vars()
        else:
            var_exists = var_str in skeleton.core.data_vars()

        var_exists = var_exists or var in core_aliases.values()
        if not data_vars or ds_var in data_vars and not var_exists:
            # 2) Find suitable coordinate group
            coords = _remap_coords(ds_var, core_coords, list(core_coords.keys()), ds, is_pointskeleton=not skeleton.is_gridded())
            # Determine coord_group
            coord_group = None
            for cg in ['spatial','all', 'grid', 'gridpoint']:
                if coords == skeleton.core.coords(cg):
                    coord_group = cg
            
            if coord_group is not None:
                settable_vars[ds_var] = (var, coord_group, coords)
    
    for ds_var, (var, coord_group, coords) in settable_vars.items():
        var_str, __ = gp.decode(var)
        core_vars[var_str] = ds_var
        coord_map[var_str] = coords

        skeleton.add_datavar(var,coord_group=coord_group)
    
    
    return skeleton, core_vars, coord_map
