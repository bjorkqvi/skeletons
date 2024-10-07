from geo_skeletons.managers.coordinate_manager import CoordinateManager
import xarray as xr
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp
from typing import Union
from geo_skeletons.errors import GridError


def identify_core_in_ds(
    core: CoordinateManager,
    ds: xr.Dataset,
    aliases: dict[Union[str, MetaParameter], str] = None,
    allowed_misses: list[str] = None,
    strict: bool = True,
) -> tuple[dict[str, str], dict[str, str], list[str], list[str]]:
    """Identify the variables in the Dataset that matches the variables in the Skeleton core

    1) If 'aliases' (core-name: ds-name) mapping is given, that is used first. Key can be either a str or a MetaParameter

    2) Tries to use the standard_name set in the geo-parameters

    3) Use trivial matching (same name in skeleton and Dataset)

    Returns:

    core_coords (dict): maps the core coordinate [str] to a Dataset coordinate [str]
    core_vars (dict): maps the core variable [str] to a Dataset variable [str]
    coord_map (dict[str, list[str]]): Gives the list of core coordinate for every core variable so that the order matches the Dataset
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
    core_vars = {'hs': 'swh'}
    # Short-long name equivalence of 'lon', 'freq' etc. hardcoded
    core_coords = {'time': 'time', 'lon': 'longitude', 'lat': 'latitude', 'freq': 'frequency'}
    coords_needed = {'lon','lat','time','freq'}
    # 'inds' not matched vs. 'x' and 'y' => 'x' non-trivial so mapped to 'inds'
    coord_map = {'hs': ['inds','time','freq']}

    Now skeleton can be initialized and data 'hs' set:
    skeleton = SkeletonClass(**core_cords)
    for var, ds_var in core_vars.items():
        skeleton.set(var, ds.get(ds_var), coords=coord_map[var])"""

    # Start by remapping any possible MetaParameters to a string s
    allowed_misses = allowed_misses or []

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

    core_vars = {}
    core_coords = {}
    coords = core.coords("init")

    for coord in coords:
        ds_coord = _get_var_from_ds(coord, aliases_str, core, ds)
        if ds_coord is not None:
            core_coords[coord] = ds_coord

    for var in core.non_coord_objects():
        ds_var = _get_var_from_ds(var, aliases_str, core, ds)
        if ds_var is not None:
            core_vars[var] = ds_var

    xy_set = core_coords.get("x") is not None and core_coords.get("y") is not None
    lonlat_set = (
        core_coords.get("lon") is not None and core_coords.get("lat") is not None
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

    missing_coords = set(coords_needed) - set(core_coords.keys())
    if not missing_coords.issubset(set(allowed_misses)) and strict:
        raise GridError(
            f"Coordinates {list(missing_coords)} not found in dataset or provided as keywords!"
        )

    coord_map = {}
    is_pointskeleton = "inds" in core.coords("all")
    for var, ds_var in core_vars.items():
        coord_map[var] = _remap_coords(
            ds_var, core_coords, coords_needed, ds, is_pointskeleton=is_pointskeleton
        )

    return core_coords, core_vars, coord_map, coords_needed


def _get_var_from_ds(var, aliases_str, core, ds):
    """Gets a given coordinate (possibly given as a geo-parameter) from a Dataset"""
    # 1) Use aliases mapping if exists
    if aliases_str.get(var) is not None:
        return aliases_str.get(var)

    # 2) Try to decode using cf standard name
    param = core.meta_parameter(var)
    if param is not None:
        ds_var = param.find_me_in_ds(ds)
        if len(ds_var) > 1:
            raise ValueError(
                f"The variable '{var}' matches {ds_var} in the Dataset. Specify which one to read by e.g. aliases = {{'{var}': '{ds_var[0]}'}}"
            )
    else:
        ds_var = []

    if ds_var:
        return ds_var[0]

    # 3) Try to see it the name is the same in the skeleton and the dataset
    if var in ds.data_vars:
        return var

    if var in ds.coords:
        return var

    # Reads a coordinate from a known list of aliases
    for alias_list in [
        LON_ALIASES,
        LAT_ALIASES,
        FREQ_ALIASES,
        DIRS_ALIASES,
        X_ALIASES,
        Y_ALIASES,
        TIME_ALIASES,
    ]:
        if var in alias_list:
            for alias_var in alias_list:
                if alias_var in ds.coords or alias_var in ds.data_vars:
                    return alias_var

    return None


def _remap_coords(
    ds_var: str,
    core_coords: dict,
    coords_needed: list[str],
    ds: xr.Dataset,
    is_pointskeleton: bool,
):
    """Maps the coordinates of a single Dataarray to the coordinates of the core variable"""

    if set(ds.get(ds_var).dims).issubset(coords_needed):
        return list(ds.get(ds_var).dims)

    # Need to rename the coordinates so they can be used in the reshape
    reversed_dict = {}
    for key, value in core_coords.items():
        reversed_dict[value] = key

    coords = []
    missed_coords = []
    max_len_of_missed_coords = 0
    for n, ds_c in enumerate(ds.get(ds_var).dims):
        core_c = reversed_dict.get(ds_c)
        if core_c in coords_needed:
            coords.append(core_c)
        else:
            coords.append(None)
            missed_coords.append((n, ds_c))
            max_len_of_missed_coords = max(max_len_of_missed_coords, len(ds.get(ds_c)))

    # Data can be given as x-y with trivial y for example
    if "inds" not in coords and is_pointskeleton:
        for n, ds_c in missed_coords:
            if len(ds.get(ds_c)) > 1 or max_len_of_missed_coords == 1:
                coords[n] = "inds"
    coords = [c for c in coords if c is not None]
    return coords


TIME_ALIASES = ["time"]
X_ALIASES = ["x"]
Y_ALIASES = ["y"]
LON_ALIASES = ["lon", "longitude"]
LAT_ALIASES = ["lat", "latitude"]
FREQ_ALIASES = ["freq", "frequency"]
DIRS_ALIASES = ["dirs", "directions", "direction", "theta"]
# Dirs can be direction from or to, so won't give a geoparameters just based on the name!


def dict_of_coords():
    """Compiles assumed aliases to map to the known geo-parameter"""
    coord_dict = {}
    for var in LON_ALIASES:
        coord_dict[var] = gp.grid.Lon
    for var in LAT_ALIASES:
        coord_dict[var] = gp.grid.Lat
    for var in X_ALIASES:
        coord_dict[var] = gp.grid.X
    for var in Y_ALIASES:
        coord_dict[var] = gp.grid.Y
    for var in FREQ_ALIASES:
        coord_dict[var] = gp.wave.Freq
    for var in DIRS_ALIASES:
        coord_dict[var] = gp.wave.Dirs
    for var in TIME_ALIASES:
        coord_dict[var] = "time"

    return coord_dict


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


def core_dicts_from_ds(ds, core_coords, core_vars, data_array: bool = False):
    """Feed the dicts from 'identify_core_in_ds' to get the actual data values as a dict

    Set data_array = True to get Dataarrays"""
    coord_dict = {}
    data_dict = {}
    for var, ds_var in core_coords.items():
        coord_dict[var] = ds.get(ds_var)
        if not data_array:
            coord_dict[var] = coord_dict[var].data

    for var, ds_var in core_vars.items():
        data_dict[var] = ds.get(ds_var)
        if not data_array:
            data_dict[var] = data_dict[var].data

    return coord_dict, data_dict
