from geo_skeletons.managers.coordinate_manager import CoordinateManager
import xarray as xr
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp
from typing import Union
from geo_skeletons.errors import GridError
from geo_skeletons.coordinate_archive import dict_of_coords


def map_ds_to_gp(
    ds: xr.Dataset,
    decode_cf: bool = True,
    aliases: dict = None,
    keep_ds_names: bool = False,
) -> tuple[dict[str, Union[str, MetaParameter]]]:
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
        else:  # Data variable
            data_vars[var] = _map_data_var(var, ds, aliases, decode_cf, keep_ds_names)
    for coord in ds.coords:
        coords[coord] = _map_coord(coord, ds, aliases, decode_cf, keep_ds_names)

    return data_vars, coords


def _map_coord(var, ds, aliases, decode_cf, keep_ds_names):
    """Maps a variable name to geo-parameter (if possible)"""
    # 1) Use given alias
    if aliases.get(var) is not None:
        if gp.is_gp_class(aliases.get(var)) and keep_ds_names:
            return aliases.get(var)(var)
        else:
            return aliases.get(var)

    # 2) Check for standard name
    if hasattr(ds[var], "standard_name") and decode_cf:
        param = gp.get(ds[var].standard_name)
        if param is not None:
            if keep_ds_names:
                return param(var)
            else:
                return param

    # 3) Use known coordinate geo-parameters or only a string_of_coords
    coord_dict = dict_of_coords()
    if coord_dict.get(var) is not None:
        if keep_ds_names:
            return coord_dict[var](var)
        else:
            return coord_dict[var]
    else:
        return var


def _map_data_var(var, ds, aliases, decode_cf, keep_ds_names):
    """Maps a variable name to geo-parameter (if possible)"""
    # 1) Use given alias
    if aliases.get(var) is not None:
        if gp.is_gp_class(aliases.get(var)) and keep_ds_names:
            return aliases.get(var)(var)
        else:
            return aliases.get(var)

    # 2) Check for standard name
    if hasattr(ds[var], "standard_name") and decode_cf:
        param = gp.get(ds[var].standard_name)
        if param is not None:
            if keep_ds_names:
                return param(var)
            else:
                return param

    # 3) Use string value
    return var


def _var_is_coordinate(var, aliases) -> bool:
    """Checks if a variable that is technicly given as a data varaible in a Dataset should actually be treated as a coordinate"""
    coord_dict = dict_of_coords()
    if var in coord_dict.keys():
        return True
    if aliases.get(var) is not None:
        if aliases.get(var) in coord_dict.keys():
            return True
        if aliases.get(var) in coord_dict.values():
            return True
    return False
