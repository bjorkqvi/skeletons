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
    verbose: bool = False,
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
                var, ds, aliases, decode_cf, keep_ds_names=False, verbose=verbose
            )
        else:  # Data variable
            data_vars[var] = _map_ds_variable_to_geo_parameter(
                var,
                ds,
                aliases,
                decode_cf,
                keep_ds_names=keep_ds_names,
                verbose=verbose,
            )

    for coord in ds.coords:
        coords[coord] = _map_ds_variable_to_geo_parameter(
            coord, ds, aliases, decode_cf, keep_ds_names=False, verbose=verbose
        )

    return data_vars, coords


def _map_ds_variable_to_geo_parameter(
    var: str,
    ds: xr.Dataset,
    aliases: dict[str, Union[str, MetaParameter]],
    decode_cf: bool,
    keep_ds_names: bool,
    verbose: bool,
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
            return_var = aliases.get(var)(var)
        elif gp.is_gp_class(aliases.get(var)):
            return_var = aliases.get(var)()
        else:
            return_var = aliases.get(var)
        if verbose:
            print(f"Given alias: {var} >> {return_var}")
        return return_var

    # 2) Check for standard name
    if hasattr(ds[var], "standard_name") and decode_cf:
        param = gp.get(ds[var].standard_name)
        if param is not None:
            if keep_ds_names:
                return_var = param(var)
            else:
                return_var = param()
            if verbose:
                print(
                    f"Standard_name: {var} [{ds[var].standard_name}] >> {return_var} [{return_var.name}]"
                )
            return return_var

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
    only_vars: list[str] = None,
    ignore_vars: list[str] = None,
):
    new_core_vars_to_ds_vars = {}

    addable_ds_vars: list[str] = _find_not_existing_vars(
        ds_vars_to_gp, core, core_vars_to_ds_vars
    )

    # Restrict to user provided variables if needed
    addable_ds_vars = set(addable_ds_vars) - set(ignore_vars)
    if only_vars:
        addable_ds_vars = addable_ds_vars.intersection(set(only_vars))
    addable_ds_vars = list(addable_ds_vars)

    for ds_var in addable_ds_vars:
        var, _ = gp.decode(ds_vars_to_gp.get(ds_var))
        new_core_vars_to_ds_vars[var] = ds_var

    xy_variables: list[tuple[MetaParameter, MetaParameter]] = (
        _find_xy_variables_present_in_ds(addable_ds_vars, ds_vars_to_gp)
    )
    xy_variables_in_core: list[MetaParameter] = _find_xy_variables_present_in_core(core)
    mag_dir_datavars_in_core: list[MetaParameter] = (
        _find_mag_dir_datavars_present_in_core(core)
    )

    mag_dirs = _find_magnitudes_and_directions_present_in_ds(
        addable_ds_vars, ds_vars_to_gp
    )

    addable_vars = _compile_list_of_addable_vars(
        xy_variables,
        xy_variables_in_core,
        mag_dirs,
        mag_dir_datavars_in_core,
        addable_ds_vars,
        ds_vars_to_gp,
    )

    addable_magnitudes = _compile_list_of_addable_magnitudes_and_directions(
        addable_vars,
        xy_variables,
        xy_variables_in_core,
        mag_dirs,
        mag_dir_datavars_in_core,
    )
    # settable_vars = _compile_list_of_settable_vars(
    #     addable_vars, addable_magnitudes, addable_ds_vars, ds_vars_to_gp
    # )

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


def _find_xy_variables_present_in_ds(
    addable_vars: list[str],
    ds_vars_to_gp: dict[str, Union[MetaParameter, str]],
) -> list[tuple[MetaParameter, MetaParameter]]:
    """Finds all the x-components that also have a corresponding y-component"""
    xy_variables = []
    for ds_var in addable_vars:
        __, var = gp.decode(ds_vars_to_gp.get(ds_var))
        if var is not None and var.i_am() == "x":
            yvar = var.my_family("y").find_me_in(
                ds_vars_to_gp.values(), return_first=True
            )
            if yvar is not None:
                xy_variables.append((var, yvar))

    return xy_variables


def _find_magnitudes_and_directions_present_in_ds(
    addable_vars: list[str],
    ds_vars_to_gp: dict[str, Union[MetaParameter, str]],
) -> list[MetaParameter]:
    """Finds all the magnitudes that also have a corresponding direction"""
    mag_dirs = []

    for ds_var in addable_vars:
        __, var = gp.decode(ds_vars_to_gp.get(ds_var))
        if var is not None and var.i_am() == "magnitude":
            dirs = var.my_family("direction").find_me_in(
                ds_vars_to_gp.values(), return_first=True
            ) or var.my_family("opposite_direction").find_me_in(
                ds_vars_to_gp.values(), return_first=True
            )
            if dirs is not None:
                mag_dirs.append((var, dirs.my_family("direction")))

    return mag_dirs


def _find_xy_variables_present_in_core(core: CoordinateManager) -> list[MetaParameter]:
    """Finds all the xy-variables that have been set to the core"""
    list_og_gps = [core.get(var).meta for var in core.data_vars()]
    xy_variables = []
    for var_str in core.data_vars():
        __, var = gp.decode(core.get(var_str).meta)
        if var is not None and var.i_am() == "x":
            yvar = var.my_family("y").find_me_in(list_og_gps, return_first=True)
            if yvar is not None:
                xy_variables.append(var)
                xy_variables.append(yvar)

    return xy_variables


def _find_mag_dir_datavars_present_in_core(
    core: CoordinateManager,
) -> list[MetaParameter]:
    """Finds all the magnitudes and directions that have been set as plain data variables in the core"""
    mag_dir_variables = []
    for var_str in core.data_vars():
        __, var = gp.decode(core.get(var_str).meta)
        if var is not None and var.i_am() in [
            "magnitude",
            "direction",
            "opposite_direction",
        ]:
            mag_dir_variables.append(var)

    return mag_dir_variables


def _xy_as_mag_dir(var, mag_dir_datavars_in_core):
    """Checks if the components are present in another form (magnitude, direction) in the core.
    Tehen they should not be added"""
    mag = var.my_family("magnitude")
    dirs = var.my_family("direction")
    op_dirs = var.my_family("opposite_direction")

    if mag is None and dirs is None and op_dirs is None:
        return False

    try:
        if var.my_family("magnitude").is_in(mag_dir_datavars_in_core):
            return True
    except AttributeError:
        breakpoint()
    if var.my_family("direction").is_in(mag_dir_datavars_in_core):
        return True

    if var.my_family("opposite_direction").is_in(mag_dir_datavars_in_core):
        return True

    return False


def _compile_list_of_addable_vars(
    xy_variables: list[tuple[MetaParameter, MetaParameter]],
    xy_variables_in_core: list[MetaParameter],
    mag_dirs: list[tuple[MetaParameter, MetaParameter]],
    mag_dir_datavars_in_core: list[MetaParameter],
    addable_ds_vars: list[str],
    ds_vars_to_gp: dict[str, MetaParameter],
) -> list[Union[MetaParameter, str]]:
    """Compiles a list of all variables that should be added as plain data variables to the new skeleton class"""

    addable_vars = []

    # Add xy-variables
    for x, y in xy_variables:
        if not x.is_in(xy_variables_in_core) and not _xy_as_mag_dir(
            x, mag_dir_datavars_in_core
        ):
            addable_vars.append(x)
        if not y.is_in(xy_variables_in_core) and not _xy_as_mag_dir(
            y, mag_dir_datavars_in_core
        ):
            addable_vars.append(y)
    # Add xy-variables corresponding to magnitudes and directions if not yet set
    for mag, dirs in mag_dirs:
        if not mag.my_family("x").is_in(addable_vars) and not mag.my_family("x").is_in(
            xy_variables_in_core
        ):
            addable_vars.append(mag.my_family("x")())
            addable_vars.append(mag.my_family("y")())

    # Add all other variables
    for ds_var in addable_ds_vars:
        var_str, var = gp.decode(ds_vars_to_gp.get(ds_var))
        if var is None:
            addable_vars.append(var_str)
        else:
            addable_vars_names = [v.name for v in addable_vars if gp.is_gp(v)] + [
                v for v in addable_vars if not gp.is_gp(v)
            ]
            if (
                not var_str in addable_vars_names
                and not _xy_as_mag_dir(var, mag_dir_datavars_in_core)
                and var.i_am()
                not in [
                    "magnitude",
                    "direction",
                    "opposite_direction",
                ]
            ):
                addable_vars.append(var)
    return addable_vars


def _compile_list_of_addable_magnitudes_and_directions(
    addable_vars: list[Union[MetaParameter, str]],
    xy_variables: list[tuple[MetaParameter, MetaParameter]],
    xy_variables_in_core: list[MetaParameter],
    mag_dirs: list[tuple[MetaParameter, MetaParameter]],
    mag_dir_datavars_in_core: list[MetaParameter],
) -> list[dict[str, MetaParameter]]:
    """Compiles a list of all magnitudes and direction that should be added to the new skeleton class

    It searches for x, and y-components in the addable_vars and creates the corresponding magnitudes and directions
    """

    # These were present in the Dataset, so use name from them if possible
    def get_param(
        variable: MetaParameter, variable_list: list[MetaParameter], var_type: str
    ):
        """Use a parameter present in dataset if possible (to get name right), otherwise use class"""
        if not variable.my_family().get(var_type).is_in(variable_list):
            return variable.my_family().get(var_type)
        else:
            for set_var in variable_list:
                if variable.my_family().get(var_type).is_same(set_var):
                    return set_var

    mags = [mag for mag, __ in mag_dirs]
    dirs = [dirs for __, dirs in mag_dirs]

    xs = [x for x, __ in xy_variables]
    ys = [y for __, y in xy_variables]

    addable_mags_and_dirs = []

    for xvar in addable_vars + xy_variables_in_core:
        __, xvar = gp.decode(xvar)
        if xvar is not None and xvar.i_am() == "x":
            yvar = xvar.my_family("y").find_me_in(
                addable_vars + xy_variables_in_core, return_first=True
            )
            if yvar is not None:
                m = xvar.my_family("magnitude").find_me_in(
                    mags, return_first=True
                ) or xvar.my_family("magnitude")
                d = xvar.my_family("direction").find_me_in(
                    dirs, return_first=True
                ) or xvar.my_family("direction")

                x = xvar.my_family("x").find_me_in(
                    xs + xy_variables_in_core, return_first=True
                ) or xvar.my_family("x")
                y = xvar.my_family("y").find_me_in(
                    ys + xy_variables_in_core, return_first=True
                ) or xvar.my_family("y")
                if not (
                    d.is_in(mag_dir_datavars_in_core)
                    or m.is_in(mag_dir_datavars_in_core)
                ):
                    mag_dict = {
                        "name": m,
                        "direction": d,
                        "x": x.name,
                        "y": y.name,
                    }
                    addable_mags_and_dirs.append(mag_dict)
    return addable_mags_and_dirs


# def _compile_list_of_settable_vars(
#     addable_vars: list[Union[MetaParameter, str]],
#     addable_magnitudes: list[dict[str, MetaParameter]],
#     addable_ds_vars: list[Union[str, MetaParameter]],
#     ds_vars_to_gp: dict[str, Union[MetaParameter, str]],
# ):
#     """Compiles a list of all variables that have data that shouldbe set"""

#     def add_var(var):
#         var_str, var = gp.decode(var)
#         if var is None:
#             exists_in_ds = var_str in ds_vars_to_gp.values()
#         else:
#             exists_in_ds = var.is_in(ds_vars_to_gp.values())

#         if exists_in_ds:
#             for ds_var, gp_var in ds_vars_to_gp.items():
#                 gp_var_str, gp_var = gp.decode(gp_var)

#                 if gp_var is None:
#                     var_found = gp_var_str == var_str
#                 else:
#                     var_found = gp_var.is_same(var)

#                 if var_found:
#                     settable_vars.append(ds_var)

#     settable_vars = []

#     for var in addable_vars:
#         add_var(var)

#     for mag_dict in addable_magnitudes:
#         for var in [mag_dict["name"], mag_dict["direction"]]:
#             add_var(var)

#     return settable_vars
