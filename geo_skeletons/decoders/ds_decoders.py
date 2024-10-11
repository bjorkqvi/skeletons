from geo_skeletons.managers.coordinate_manager import CoordinateManager
import xarray as xr
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp
from typing import Union
from geo_skeletons.errors import GridError
from geo_skeletons.variable_archive import coord_alias_map_to_gp, var_alias_map_to_gp
from copy import deepcopy


def map_ds_to_gp(
    ds: xr.Dataset,
    decode_cf: bool = True,
    aliases: dict = None,
    keep_ds_names: bool = False,
) -> tuple[dict[str, Union[str, MetaParameter]]]:
    """Maps data variables in the dataset to geoparameters

    1) Use any alias explicitly given in dict 'aliases', e.g. aliases={'hs', gp.wave.Hs} maps ds-variable 'hs' to gp.wave.Hs

    2) Checks if a 'standard_name' is present and matches a geo-parameter (disable with decode_cf = False)

    3) Use known aliases of the parameter (e.g. 'longitude' known aliase of 'lon')

    4) Uses the variable name as is

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
            coords[var] = _map_ds_variable_to_geo_parameter(
                var, ds, aliases, decode_cf, keep_ds_names=False
            )
        else:  # Data variable
            data_vars[var] = _map_ds_variable_to_geo_parameter(
                var,
                ds,
                aliases,
                decode_cf,
                keep_ds_names=keep_ds_names,
            )

    for coord in ds.coords:
        coords[coord] = _map_ds_variable_to_geo_parameter(
            coord, ds, aliases, decode_cf, keep_ds_names=False
        )

    return data_vars, coords


def _map_ds_variable_to_geo_parameter(
    var: str,
    ds: xr.Dataset,
    aliases: dict[str, Union[str, MetaParameter]],
    decode_cf: bool,
    keep_ds_names: bool,
) -> Union[MetaParameter, str]:
    """Maps a variable name to geo-parameter (if possible)

    1) If an alias is given, use that.
        - If a geo-parameter class is given and 'keep_ds_names' = True, then the Dataset name will be used.

    2) Find a geo-parameter based on the standard name in the Dataset
        - Only if 'decode_cf' = True

    3) Use known aliases that map to known geo-parameters
        - 'keep_ds_names' = True, then the Dataset name will be used.

    1, 2 & 3: If 'keep_ds_names' = True, then the Dataset name will be used (for 1) a class needs to be given, not an instance)

    Example: Dataset had variabel 'hsig'

    1) If e.g. aliases {'hsig': gp.wave.Hs} is defined
        - Return gp.wave.Hs('hsig') if 'keep_ds_names' = True, gp.wave.Hs() otherwise
        - NB! If gp.wave.Hs() is given, then it cannot be initialized with 'hsig' even id 'keep_ds_names' = True!

    2) If a standard name is defined in the Dataset, then find the matching geo-parameter gp.wave.Hs
        - Return gp.wave.Hs('hsig') if 'keep_ds_names' = True, gp.wave.Hs() otherwise

    3) Return 'hsig', since no known aliases are defined


    Example: Dataset had coordinate 'longitude'

    1) If e.g. aliases {'longitude': gp.grid.Lon} is defined
        - Return gp.grid.Lon()

    2) If a standard name is defined in the Dataset, then find the matching geo-parameter gp.grid.Lon
        - Return gp.grid.Lon()

    3) Return gp.grid.Lon(), since 'longitude' is a known alias of 'lon' and they map to gp.grid.Lon
    """

    # 1) Use given alias
    if aliases.get(var) is not None:
        if gp.is_gp_class(aliases.get(var)) and keep_ds_names:
            return aliases.get(var)(var)
        elif gp.is_gp_class(aliases.get(var)):
            return aliases.get(var)()
        else:
            return aliases.get(var)

    # 2) Check for standard name
    if hasattr(ds[var], "standard_name") and decode_cf:
        param = gp.get(ds[var].standard_name)
        if param is not None:
            if keep_ds_names:
                return param(var)
            else:
                return param()

    # 3) Use known coordinate geo-parameters or only a string_of_coords
    for alias_dict in [coord_alias_map_to_gp(), var_alias_map_to_gp()]:
        param = alias_dict.get(var.lower())
        if param is not None:
            if gp.is_gp(param):
                if keep_ds_names:
                    return param(var)
                else:
                    return param()
            else:
                return param

    # Return string as is
    return var


def _var_is_coordinate(var, aliases) -> bool:
    """Checks if a variable that is technicly given as a data varaible in a Dataset should actually be treated as a coordinate"""
    var = var.lower()
    coord_dict = coord_alias_map_to_gp()
    if var in coord_dict.keys():
        return True
    if aliases.get(var) is not None:
        if aliases.get(var) in coord_dict.keys():
            return True
        if aliases.get(var) in coord_dict.values():
            return True
    return False


def find_addable_vars_and_magnitudes(
    core,
    ds_vars_to_gp: dict[str, Union[str, MetaParameter]],
    core_vars_to_ds_vars: dict[str, str],
    data_vars: list[str] = None,
    ignore_vars: list[str] = None,
):
    new_core_vars_to_ds_vars = {}

    addable_vars = _find_not_existing_vars(ds_vars_to_gp, core, core_vars_to_ds_vars)

    addable_vars = set(addable_vars) - set(ignore_vars)
    if data_vars:
        addable_vars = addable_vars.intersection(set(data_vars))
    addable_vars = list(addable_vars)

    for ds_var in addable_vars:
        var, _ = gp.decode(ds_vars_to_gp.get(ds_var))
        new_core_vars_to_ds_vars[var] = ds_var

    x_variables = _find_x_variables(addable_vars, ds_vars_to_gp)
    addable_magnitudes = _find_addable_magnitudes(x_variables)

    return addable_vars, addable_magnitudes, new_core_vars_to_ds_vars


def _find_not_existing_vars(
    ds_vars_to_gp: dict[str, Union[str, MetaParameter]],
    core: CoordinateManager,
    core_vars_to_ds_vars: dict[str, str],
) -> list[str]:
    """Find all the variables in the Dataset that don't already exist in the Skeleton"""
    new_vars = []

    for ds_var, var in ds_vars_to_gp.items():
        # 1) Check if variable exists
        var_str, __ = gp.decode(var)
        if __ is not None:
            var_exists = (
                core.find_cf(var.standard_name()) != [] or var_str in core.data_vars()
            )
        else:
            var_exists = (
                var_str in core_vars_to_ds_vars.keys()
                or var_str in core_vars_to_ds_vars.values()
            )

        if not var_exists:
            new_vars.append(ds_var)

    return new_vars


def _find_x_variables(
    addable_vars: list[str], ds_vars_to_gp: dict[str, Union[MetaParameter, str]]
) -> list[MetaParameter]:
    """Finds all the x-components that also have a corresponding y-component"""
    x_variables = []
    for ds_var in addable_vars:
        var_str, var = gp.decode(ds_vars_to_gp.get(ds_var))
        if var is not None:
            if var.i_am() == "x":
                # Check for matching y-variable
                y_ok = False
                for v in ds_vars_to_gp.values():
                    __, v = gp.decode(v)
                    if v is not None:
                        if var.my_family().get("y").is_same(v):
                            y_ok = True
                if y_ok:
                    x_variables.append(var)
    return x_variables


def _find_addable_magnitudes(
    x_variables: list[MetaParameter],
) -> list[tuple[MetaParameter, MetaParameter]]:
    # Search for possibilities to set magnitude and direction
    addable_magnitudes = []
    for var in x_variables:
        addable_magnitudes.append(
            (var.my_family().get("mag"), var.my_family().get("dir"))
        )
    return addable_magnitudes
