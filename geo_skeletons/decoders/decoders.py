from geo_skeletons.managers.coordinate_manager import CoordinateManager
import xarray as xr
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp
from typing import Union

def identify_core_in_ds(core: CoordinateManager, ds: xr.Dataset, aliases: dict[Union[str, MetaParameter], str] = None) -> dict[str, str]:
    """Identify the variables in the Dataset that matches the variables in the Skeleton core

    1) If 'aliases' (core-name: ds-name) mapping is given, that is used first. Key can be either a str or a MetaParameter
    
    2) Tries to use the standard_name set in the geo-parameters
    
    3) Use trivial matching (same name in skeleton and Dataset)"""
   
    # Start by remapping any possible MetaParameters to a string s
    aliases_str = {}
    if aliases is not None:
        for core_var, ds_var in aliases.items():
            name, param = gp.decode(core_var)
            if param is not None:
                name = core.find_cf(param.standard_name())
                if name is not None and len(name) == 1: # Found exactly one matching name
                    if ds_var in ds.data_vars: # Only add it if it actually exists
                        aliases_str[name[0]] = ds_var
            else:
                if ds_var in ds.data_vars: # Only add it if it actually exists
                    aliases_str[name] = ds_var

    
   
    core_vars = {}
    core_coords = {}
    coords = core.data_vars('spatial') or core.coords()
    for coord in coords:
        ds_coord = _get_var_from_ds(coord, aliases_str, core, ds)
        if ds_coord is not None:
            core_coords[coord] = ds_coord

    for var in core.data_vars():
        ds_var =  _get_var_from_ds(var, aliases_str, core, ds)
        if ds_var is not None:
            core_vars[var] = ds_var

    return core_vars, core_coords


def _get_var_from_ds(var, aliases_str, core, ds):
    # 1) Use aliases mapping if exists
    if aliases_str.get(var) is not None:
        return aliases_str.get(var)
        
    # 2) Try to decode using cf standard name
    param = core.meta_parameter(var)
    if param is not None:
        ds_var = param.find_me_in_ds(ds)
        if len(ds_var) > 1:
            raise ValueError(f"The variable '{var}' matches {ds_var} in the Dataset. Specify which one to read by e.g. aliases = {{'{var}': '{ds_var[0]}'}}")
    else:
        ds_var = []

    if ds_var:
        return ds_var[0]

    # 3) Try to see it the name is the same in the skeleton and the dataset
    if var in ds.data_vars:
        return var

    return None

# Dirs can be direction from or to, so won't give a geoparameters just based on the name!
DICT_OF_COORDS = {'lon': gp.grid.Lon, 'longitude': gp.grid.Lon, 'lat': gp.grid.Lat, 'latitude': gp.grid.Lat, 'x': gp.grid.X, 'y': gp.grid.Y, 'freq': gp.wave.Freq, 'frequency': gp.wave.Freq}

def map_ds_to_gp(ds: xr.Dataset, decode_cf: bool = True, aliases: dict=None, keep_ds_names: bool=False) -> tuple[dict[str, Union[str, MetaParameter]]]:
    """Maps data variables in the dataset to geoparameters
    
    1) Use any alias explicitly given in dict 'aliases', e.g. aliases={'hs', gp.wave.Hs} maps ds-variable 'hs' to gp.wave.Hs
    
    2) Checks if a 'standard_name' is present and matches a geo-parameter (disable with decode_cf = False)
    
    3) Uses the variable name as is

    keep_ds_names = True: Initialize the possible geo-parameters with the name of the Dataset variable
    
    Returns data variable and coordinates separately"""
    if aliases is None:
        aliases = {}

    data_vars = {}
    coords = {}
    for var in ds.data_vars:
        # Coordinates can be listed as data variables in unstructured datasets
        # Check if we are dealing with a coordinate
        if _var_is_coordinate(var, aliases): 
            coords[var] = _map_coord(var, ds, aliases, decode_cf, keep_ds_names)
        else: # Data variable
            data_vars[var] = _map_data_var(var, ds, aliases, decode_cf, keep_ds_names)
    
    for coord in ds.coords:
        coords[coord] = _map_coord(coord, ds, aliases, decode_cf, keep_ds_names)

    return data_vars, coords


def _map_coord(var, ds, aliases, decode_cf, keep_ds_names):
    # 1) Use given alias
    if aliases.get(var) is not None:
        if gp.is_gp_class(aliases.get(var)) and keep_ds_names:
            return aliases.get(var)(var)
        else:
            return aliases.get(var)
        
    # 2) Check for standard name
    if hasattr(ds[var], 'standard_name') and decode_cf:
        param = gp.get(ds[var].standard_name)
        if param is not None:
            if keep_ds_names:
                return param(var)
            else:
                return param

    
    # 3) Use known coordinate geo-parameters or only a string
    if DICT_OF_COORDS.get(var) is not None:
        if keep_ds_names:
            return DICT_OF_COORDS[var](var)
        else:
            return DICT_OF_COORDS[var]
    else:
        return var


def _map_data_var(var, ds, aliases, decode_cf, keep_ds_names):
    # 1) Use given alias
    if aliases.get(var) is not None:
        if gp.is_gp_class(aliases.get(var)) and keep_ds_names:
            return aliases.get(var)(var)
        else:
            return aliases.get(var)
   
    # 2) Check for standard name
    if hasattr(ds[var], 'standard_name') and decode_cf:
        param = gp.get(ds[var].standard_name)
        if param is not None:
            if keep_ds_names:
                return param(var)
            else:
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