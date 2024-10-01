from geo_skeletons.managers.coordinate_manager import CoordinateManager
import xarray as xr
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp

def identify_core_in_ds(skeleton, ds: xr.Dataset, core_to_ds: dict[str | MetaParameter, str] = None) -> dict[str, str]:
    """Identify the variables in the Dataset that matches the variables in the Skeleton core

    If 'core_to_ds' mapping is given, that is used first. Key can be either a str or a MetaParameter
    
    Otherwise primarily tries to use the standard_name set bu the geo-parameters
    
    if not found, tries to use trivial matching (same name in skeleton and Dataset)"""
   
    # Start by remapping any possible MetaParameters to a string name
    core_to_ds_str = {}
    if core_to_ds is not None:
        for core_var, ds_var in core_to_ds.items():
            name, param = gp.decode(core_var)
            if param is not None:
                name = skeleton.find_cf(param.standard_name())
                if name is not None and len(name) == 1: # Found exactly one matching name
                    if ds_var in ds.data_vars: # Only add it if it actually exists
                        core_to_ds_str[name[0]] = ds_var
            else:
                if ds_var in ds.data_vars: # Only add it if it actually exists
                    core_to_ds_str[name] = ds_var

    
   
    core_vars = {}
    for var in skeleton.core.data_vars():
        # 1) Use core_to_ds mapping if exists
        if core_to_ds_str.get(var) is not None:
            core_vars[var] = core_to_ds_str.get(var)
            continue


        # 2) Try to decode using cf standard name
        param = skeleton.core.meta_parameter(var)
        if param is not None:
            ds_var = param.find_me_in_ds(ds)
        else:
            ds_var = None

        if ds_var is not None:
            core_vars[var] = ds_var
            continue

        # 3) Try to see it the name is the same in the skeleton and the dataset
        if var in ds.data_vars:
            core_vars[var] = var

    return core_vars
    