from geo_skeletons.managers.coordinate_manager import CoordinateManager
import xarray as xr
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp
from typing import Union

def identify_core_in_ds(skeleton, ds: xr.Dataset, core_to_ds: dict[Union[str, MetaParameter], str] = None) -> dict[str, str]:
    """Identify the variables in the Dataset that matches the variables in the Skeleton core

    1) If 'core_to_ds' mapping is given, that is used first. Key can be either a str or a MetaParameter
    
    2) Tries to use the standard_name set bu the geo-parameters
    
    3) Use trivial matching (same name in skeleton and Dataset)"""
   
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

DICT_OF_COORDS = {'lon': gp.grid.Lon, 'longitude': gp.grid.Lon, 'lat': gp.grid.Lat, 'latitude': gp.grid.Lat, 'x': gp.grid.X, 'y': gp.grid.Y}

def map_ds_to_gp(ds: xr.Dataset, decode_cf: bool = True, aliases: dict=None) -> tuple[dict[str, Union[str, MetaParameter]]]:
    """Maps data variables in the dataset to geoparameters
    
    1) Use any alias explicitly given in dict 'aliases', e.g. aliases={'hs', gp.wave.Hs} maps ds-variable 'hs' to gp.wave.Hs
    
    2) Checks if a 'standard_name' is present and matches a geo-parameter (disable with decode_cf = False)
    
    3) Uses the variable name as is
    
    Returns data variable and coordinates separately"""
    if aliases is None:
        aliases = {}

    data_vars = {}
    coords = {}
    for var in ds.data_vars:
        # Coordinates can be listed as data variables in unstructured datasets
        # Check if we are dealing with a coordinate
        if _var_is_coordinate(var, aliases): 
            coords[var] = _map_coord(var, ds, aliases, decode_cf)
        else: # Data variable
            data_vars[var] = _map_data_var(var, ds, aliases, decode_cf)
    
    for coord in ds.coords:
        coords[coord] = _map_coord(coord, ds, aliases, decode_cf)

    return data_vars, coords


def _map_coord(var, ds, aliases, decode_cf):
    # 1) Use given alias
    if aliases.get(var) is not None:
        return aliases.get(var)
        
    # 2) Check for standard name
    if hasattr(ds[var], 'standard_name') and decode_cf:
        param = gp.get(ds[var].standard_name)
        if param is not None:
            return param
    
    # 3) Use known coordinate geo-parameters or only a string
    if DICT_OF_COORDS.get(var) is not None:
        return DICT_OF_COORDS[var]
    else:
        return var


def _map_data_var(var, ds, aliases, decode_cf):
    # 1) Use given alias
    if aliases.get(var) is not None:
        return aliases.get(var)
   
    # 2) Check for standard name
    if hasattr(ds[var], 'standard_name') and decode_cf:
        param = gp.get(ds[var].standard_name)
        if param is not None:
            return param

    # 3) Use string value
    return var

def _var_is_coordinate(var, aliases) -> bool:
    if var in DICT_OF_COORDS.keys():
        return True
    if aliases.get(var) is not None:
        if aliases.get(var) in DICT_OF_COORDS.keys():
            return True
        if aliases.get(var) in DICT_OF_COORDS.values():
            return True
    return False