from geo_skeletons.managers.coordinate_manager import CoordinateManager
import xarray as xr
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp
from typing import Union
from geo_skeletons.errors import GridError
from geo_skeletons.variable_archive import coord_alias_map_to_gp, var_alias_map_to_gp
from .core_decoders import _remap_coords
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


def find_settable_vars_and_magnitudes(
    skeleton_class,
    ds: xr.Dataset,
    mapped_vars: dict[str, str],
    core_coords: dict[str, str],
    core_vars: dict[str, str],
    data_vars: list[str] = None,
    ignore_vars: list[str] = None,
    lon_had_time_dim: bool = False,
):

    settable_vars = _find_settable_vars(
        ds,
        mapped_vars,
        skeleton_class,
        core_coords,
        core_vars,
        data_vars,
        ignore_vars,
        lon_had_time_dim,
    )

    x_variables = _find_x_variables(settable_vars)

    settable_magnitudes = _find_settable_magnitudes(x_variables)

    return settable_vars, settable_magnitudes


def _find_settable_vars(
    ds: xr.Dataset,
    mapped_vars: dict[str, Union[str, MetaParameter]],
    skeleton_class,
    core_coords: dict,
    core_vars: dict[str, str],
    data_vars: list[str],
    ignore_vars: list[str],
    lon_had_time_dim: bool,
) -> dict[Union[str, MetaParameter], str, list[str]]:
    """Find all the variables in the Dataset that can e added to the skeleton_class.

    Reasons not to add:
    1) It already exists (based on name or standard_name of geo-parameter)
    2) We can't find a coordinate groupt that fits the dimensions"""
    settable_vars = {}
    cartesian = core_coords.get("x") is not None

    for ds_var, var in mapped_vars.items():
        # 1) Check if variable exists
        var_str, __ = gp.decode(var)
        if __ is not None:
            var_exists = (
                skeleton_class.core.find_cf(var.standard_name()) != []
                or var_str in skeleton_class.core.data_vars()
            )
        else:
            var_exists = var_str in core_vars.keys() or var_str in core_vars.values()

        # var_exists = var_exists or var in core_aliases.values()

        if (
            (not data_vars or ds_var in data_vars)
            and (not var_exists)
            and (ds_var not in ignore_vars)
        ):
            # 2) Find suitable coordinate group
            coords = _remap_coords(
                ds_var,
                core_coords,
                skeleton_class.core.coords("init"),
                ds,
                is_pointskeleton=not skeleton_class.is_gridded(),
            )

            # Determine coord_group
            coord_group = None
            for cg in ["spatial", "all", "grid", "gridpoint"]:
                compare_coords = deepcopy(coords)
                skeleton_coord_group = skeleton_class.core.coords(cg)
                if lon_had_time_dim and "time" in skeleton_coord_group:
                    if skeleton_class.is_gridded():
                        if cartesian:
                            compare_coords.append("y")
                            compare_coords.append("x")
                        else:
                            compare_coords.append("lat")
                            compare_coords.append("lon")
                    else:
                        compare_coords.append("inds")
                if cartesian:
                    skeleton_coord_group = list(
                        map(lambda x: x.replace("lat", "y"), skeleton_coord_group)
                    )
                    skeleton_coord_group = list(
                        map(lambda x: x.replace("lon", "x"), skeleton_coord_group)
                    )
                else:
                    skeleton_coord_group = list(
                        map(lambda x: x.replace("y", "lat"), skeleton_coord_group)
                    )
                    skeleton_coord_group = list(
                        map(lambda x: x.replace("x", "lon"), skeleton_coord_group)
                    )
                if compare_coords == skeleton_coord_group:
                    coord_group = cg

            if coord_group is not None:
                settable_vars[ds_var] = (var, coord_group, coords)
    return settable_vars


def _find_x_variables(
    settable_vars: list[tuple[Union[str, MetaParameter], str, list[str]]]
):
    x_variables = []
    gps_to_set = []
    for _, (var, __, ___) in settable_vars.items():
        gps_to_set.append(var)

    for ds_var, (var, coord_group, coords) in settable_vars.items():
        var_str, var = gp.decode(var)
        if var is not None:
            if var.i_am() == "x":
                # Check for matching y-variable
                y_ok = False
                for v in gps_to_set:
                    if var.my_family().get("y").is_same(v):
                        y_ok = True
                if y_ok:
                    x_variables.append(var)
    return x_variables


def _find_settable_magnitudes(
    x_variables: list[MetaParameter],
) -> list[tuple[MetaParameter, MetaParameter]]:
    # Search for possibilities to set magnitude and direction
    settable_magnitudes = []
    for var in x_variables:
        settable_magnitudes.append(
            (var.my_family().get("mag"), var.my_family().get("dir"))
        )
    return settable_magnitudes
