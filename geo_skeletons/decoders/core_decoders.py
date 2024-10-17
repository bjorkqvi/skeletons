from geo_skeletons.managers.coordinate_manager import CoordinateManager
import xarray as xr
import numpy as np
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp
from typing import Union
from geo_skeletons.errors import GridError

# from geo_skeletons.variable_archive import (
#     LON_ALIASES,
#     LAT_ALIASES,
#     X_ALIASES,
#     Y_ALIASES,
#     TIME_ALIASES,
#     FREQ_ALIASES,
#     DIRS_ALIASES,
# )
from geo_skeletons.variable_archive import (
    LIST_OF_COORD_ALIASES,
    LIST_OF_VAR_ALIASES,
    coord_alias_map_to_gp,
    var_alias_map_to_gp,
)
from geo_skeletons.dir_conversions import compute_magnitude, compute_math_direction


def identify_core_in_ds(
    core: CoordinateManager,
    ds: xr.Dataset,
    aliases: dict[Union[str, MetaParameter], str] = None,
    allowed_misses: list[str] = None,
    decode_cf: bool = True,
    strict: bool = True,
) -> tuple[dict[str, str], dict[str, str], dict[str, list[str]], list[str]]:
    """Identify the variables in the Dataset that matches the variables in the Skeleton core

    1) If 'aliases' (core-name: ds-name) mapping is given, that is used first. Key can be either a str or a MetaParameter

    2) Tries to use the standard_name set in the geo-parameters

    3) Use trivial matching (same name in skeleton and Dataset)

    Returns:

    core_coords_to_ds_coords (dict): maps the core coordinate [str] to a Dataset coordinate [str]
    core_vars_to_ds_vars (dict): maps the core variable [str] to a Dataset variable [str]
    coords_needed (list [str]): list of all coordinate names that are needed to succsessfully initialize the core

    Ex.
    Skeleton has:
     - one variables gp.wave.Hs('hs')
     - defined over 'time', 'inds', 'freq'
     - 'lon' and 'lat' specifying the 'inds'

     Dataset has:
      - one variable 'swh' with standard_name 'sea_surface_wave_significant_height'
      - defined over 'x', 'y', 'time', 'frequency'
      - 'x' defines number of points and 'y' is trivial
      - 'longitude', 'latitude' defined over 'x' and 'y'

    # Based on matching standard_name to geo-parameter
    core_vars_to_ds_vars = {'hs': 'swh'}
    # Short-long name equivalence of 'lon', 'freq' etc. hardcoded
    core_coords_to_ds_coords = {'time': 'time', 'lon': 'longitude', 'lat': 'latitude', 'freq': 'frequency'}
    coords_needed = {'lon','lat','time','freq'}

    Now skeleton can be initialized and data 'hs' set:
    skeleton = SkeletonClass(**core_cords)
    for var, ds_var in core_vars_to_ds_vars.items():
        skeleton.set(var, ds.get(ds_var), coords=coord_map[var])"""

    # Start by remapping any possible MetaParameters to a string
    allowed_misses = allowed_misses or []

    aliases = _remap_core_aliase_keys_to_strings(aliases, core, ds)

    core_vars_to_ds_vars = {}
    core_coords_to_ds_coords = {}
    coords = core.coords("init")

    for coord in coords:
        search_param = core.meta_parameter(coord) or coord
        ds_coord = _map_geo_parameter_to_ds_variable(search_param, ds, aliases)
        if ds_coord is not None:
            core_coords_to_ds_coords[coord] = ds_coord

    for var in core.data_vars():
        search_param = core.meta_parameter(var) or var
        # Find the parameter straight up
        ds_var_x = _map_geo_parameter_to_ds_variable(search_param, ds, aliases)
        if ds_var_x is not None:
            core_vars_to_ds_vars[var] = ds_var_x
            continue

        if not decode_cf:
            continue

        # Find e.g. fp when we want Tp, or WindDirTo when we want WindDir
        ds_var_x, transform_function, dir_type = _get_inverse_parameter_from_ds(
            search_param, ds
        )
        if ds_var_x is not None:
            core_vars_to_ds_vars[var] = (
                ds_var_x,
                None,
                transform_function,
                dir_type,
            )
            continue

        # Find e.g. x_wind and y_wind when we want Wind or WindDir
        ds_var_x, ds_var_y, transform_function, dir_type = _get_components_from_ds(
            search_param, ds
        )
        if ds_var_x is not None:
            core_vars_to_ds_vars[var] = (
                ds_var_x,
                ds_var_y,
                transform_function,
                dir_type,
            )

    for var in core.magnitudes() + core.directions():
        search_param = core.meta_parameter(var) or var

        ds_var = _map_geo_parameter_to_ds_variable(search_param, ds, aliases)

        if ds_var is not None:
            core_vars_to_ds_vars[var] = ds_var
            continue

        ds_var_x, transform_function, dir_type = _get_inverse_parameter_from_ds(
            search_param, ds
        )
        if ds_var_x is not None:
            core_vars_to_ds_vars[var] = (
                ds_var_x,
                None,
                transform_function,
                dir_type,
            )

    xy_set = (
        core_coords_to_ds_coords.get("x") is not None
        and core_coords_to_ds_coords.get("y") is not None
    )
    lonlat_set = (
        core_coords_to_ds_coords.get("lon") is not None
        and core_coords_to_ds_coords.get("lat") is not None
    )

    grid_miss_allowed = ("x" in allowed_misses and "y" in allowed_misses) or (
        "lon" in allowed_misses and "lat" in allowed_misses
    )

    if not lonlat_set and not xy_set:
        if strict and not grid_miss_allowed:
            raise GridError("Can't find x/y lon/lat pair in Dataset!")
        # Remove the unused pari x/y or lon/lat
        # Both lon/lat and x/y can be present. Then use lon/lat, since x/y can just be a bad version of x=inds and y=trivial
        coords_needed = core.coords("init")
    else:
        coords_needed = core.coords("init", cartesian=(not lonlat_set))

    missing_coords = set(coords_needed) - set(core_coords_to_ds_coords.keys())
    if not missing_coords.issubset(set(allowed_misses)) and strict:
        raise GridError(
            f"Coordinates {list(missing_coords)} not found in dataset or provided as keywords!"
        )

    return (
        core_coords_to_ds_coords,
        core_vars_to_ds_vars,
        coords_needed,
    )


def gather_coord_values(
    coords_needed: list[str],
    ds: xr.Dataset,
    core_coords_to_ds_coords: dict[str, str],
    extra_coords: dict[str, Union[np.ndarray, xr.DataArray]],
) -> dict[str, np.ndarray]:
    """Gathers the coordinate values from the xarray Dataset and appends in any missing values that are provided in the extra_coords dict

    If lon/lat or x/y has a time dimension, that is removed with nanmedian"""
    coords = {}

    for coord in coords_needed:
        val = (
            ds.get(core_coords_to_ds_coords.get(coord))
            if core_coords_to_ds_coords.get(coord) is not None
            else extra_coords.get(coord)
        )

        if isinstance(val, xr.DataArray):
            if coord in ["x", "y", "lon", "lat"] and "time" in val.dims:
                val = val.median(dim="time", skipna=True)
            val = val.data

        coords[coord] = val

    return coords


def _map_geo_parameter_to_ds_variable(
    param: MetaParameter,
    ds: xr.Dataset,
    aliases: dict[str, str] = None,
) -> Union[str, None]:
    """Gets a given coordinate from a Dataset:

    1) If a explicit alias mapping is given, return that.
    2) Try using standard_name matching from possible geo-parameter in the core (if decode_cf = True [Default])
    3) Try to match 'var' exactly to something in the Dataset
    4) Try to match known aliases of 'var' to something in the Dataset
    5) Return None if not found

    E.g. var = 'lon'
    1) See if e.g. aliases['lon'] = 'some_other_name_in_ds' is defined
    2) Get the geo-parameter gp.grid.Lon from the core and match the standard name 'longitude' to standard names in the Dataset
    3) Try to find 'lon' directly in eiher ds.data_vars or ds.coords
    4) Go through known aliases of 'lon' (e.g. 'longitude') and try to find the alias 'longitude' in eiher ds.data_vars or ds.coords
    """

    aliases = aliases or {}
    var_str, param = gp.decode(param, init=True)

    # 1) Use aliases mapping if exists
    if aliases.get(var_str) is not None:
        return aliases.get(var_str)

    if param is None:
        param = coord_alias_map_to_gp().get(var_str) or var_alias_map_to_gp().get(
            var_str
        )
        if not gp.is_gp(param):
            param = None

    if param is not None:
        ds_var = _get_geoparameter_from_ds(param, ds)

        if ds_var:
            return ds_var

    # 3) Try to see it the name is the same in the skeleton and the dataset
    # Don't do this name matching for directional variables because of the from-to ambiguity
    if param is None or param.dir_type is None:
        if var_str in ds.data_vars:
            return var_str

    if var_str in ds.coords:
        return var_str

    # 4) Reads from a known list of aliases
    for alias_list in LIST_OF_COORD_ALIASES + LIST_OF_VAR_ALIASES:
        if var_str in alias_list:  # E.g. 'lon' in LON_ALIASES
            for alias_var in alias_list:  # E.g. LON_ALIASES = ['lon','lognitude']
                # E.g. 'longitude' in ds.data_vars
                if alias_var in ds.coords or alias_var in ds.data_vars:
                    return alias_var

    return None


def _get_inverse_parameter_from_ds(var: Union[MetaParameter, str], ds):
    """Get inverse from Dataset (e.g. get fp if we want Tp)"""
    if not gp.is_gp(var):
        var = var_alias_map_to_gp().get(var) or coord_alias_map_to_gp().get(var)
    if var is None:
        return None, None, None

    if var.i_am() == "period":
        ds_var = _get_geoparameter_from_ds(var.my_family("frequency"), ds)
        transform_function = lambda x, y: 1 / x
        dir_type = None
    elif var.i_am() == "frequency":
        ds_var = _get_geoparameter_from_ds(var.my_family("period"), ds)
        transform_function = lambda x, y: 1 / x
        dir_type = None
    elif var.i_am() == "direction":
        ds_var = _get_geoparameter_from_ds(var.my_family("opposite_direction"), ds)
        transform_function = lambda x, y: x
        dir_type = var.my_family("opposite_direction").dir_type()
    elif var.i_am() == "opposite_direction":
        ds_var = _get_geoparameter_from_ds(var.my_family("direction"), ds)
        transform_function = lambda x, y: x
        dir_type = var.my_family("direction").dir_type()
    else:
        return None, None, None

    if ds_var is not None:
        return ds_var, transform_function, dir_type

    return None, None, None


def _get_components_from_ds(var: Union[MetaParameter, str], ds):
    """Get components from Dataset (e.g. get x_wind, y_wind if we want wind_speed)"""
    if not gp.is_gp(var):
        var = var_alias_map_to_gp().get(var) or coord_alias_map_to_gp().get(var)
    if var is None:
        return None, None, None

    if var.i_am() in ["magnitude", "direction", "opposite_direction"]:
        ds_var_x = _get_geoparameter_from_ds(var.my_family("x"), ds)
        ds_var_y = _get_geoparameter_from_ds(var.my_family("y"), ds)
    else:
        return None, None, None, None

    if var.i_am() == "magnitude":
        transform_function = compute_magnitude
        dir_type = None
    elif var.i_am() in ["direction", "opposite_direction"]:
        transform_function = compute_math_direction
        dir_type = "math"
    if ds_var_x is not None and ds_var_y is not None:
        return ds_var_x, ds_var_y, transform_function, dir_type

    return None, None, None, None


def _get_geoparameter_from_ds(param: MetaParameter, ds: xr.Dataset) -> Union[str, None]:
    """Finds a geo-parameter from a Dataset and returns the variable name if a unique match is made"""
    if param is None:
        return None

    ds_var = param.find_me_in_ds(ds)

    if not ds_var:
        return None

    if len(ds_var) == 1:
        return ds_var[0]

    ds_var = [
        dv for dv in ds_var if param.name == ds_var
    ]  # See if we have a perfect name match

    if len(ds_var) == 1:
        return ds_var[0]

    raise ValueError(
        f"The variable '{param.name}' matches {ds_var} in the Dataset. Specify which one to read by e.g. aliases = {{'{param.name}': '{ds_var[0]}'}}"
    )


def _remap_core_aliase_keys_to_strings(
    aliases: dict[Union[str, MetaParameter], str],
    core: CoordinateManager,
    ds: xr.Dataset,
) -> dict[str, str]:
    """core_aliases migh be given as e.g. {gp.wave.Hs: 'hsig'}
    If the name in the core for gp.wave.Hs is 'hs', then we remap to {'hs': 'hsig'}
    """
    aliases_str = {}
    if aliases is not None:
        for core_var, ds_var in aliases.items():
            name, param = gp.decode(core_var)
            if param is not None:
                name = core.find_cf(param.standard_name())
                if (
                    name is not None and len(name) == 1
                ):  # Found exactly one matching name
                    if ds_var in ds.data_vars:  # Only add it if it actually exists
                        aliases_str[name[0]] = ds_var
            else:
                if ds_var in ds.data_vars:  # Only add it if it actually exists
                    aliases_str[name] = ds_var
    return aliases_str
