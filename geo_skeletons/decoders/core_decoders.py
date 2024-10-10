from geo_skeletons.managers.coordinate_manager import CoordinateManager
import xarray as xr
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp
from typing import Union
from geo_skeletons.errors import GridError
from geo_skeletons.variable_archive import (
    LON_ALIASES,
    LAT_ALIASES,
    X_ALIASES,
    Y_ALIASES,
    TIME_ALIASES,
    FREQ_ALIASES,
    DIRS_ALIASES,
)


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

    # Start by remapping any possible MetaParameters to a string
    allowed_misses = allowed_misses or []

    aliases = _remap_core_aliase_keys_to_strings(aliases, core, ds)

    core_vars = {}
    core_coords = {}
    coords = core.coords("init")

    for coord in coords:
        ds_coord = _get_var_from_ds(coord, aliases, core, ds, decode_cf)
        if ds_coord is not None:
            core_coords[coord] = ds_coord

    for var in core.non_coord_objects():
        ds_var = _get_var_from_ds(var, aliases, core, ds, decode_cf)
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

    # is_pointskeleton = "inds" in core.coords("all")
    # coord_map_for_vars = {}
    # for var, ds_var in core_vars.items():
    #     coord_map_for_vars[var] = _remap_coords(
    #         ds_var, core_coords, coords_needed, ds, is_pointskeleton=is_pointskeleton
    #     )

    # coord_map_for_coords = {}
    # for var, ds_var in core_coords.items():
    #     coord_map_for_coords[var] = _remap_coords(
    #         ds_var, core_coords, coords_needed, ds, is_pointskeleton=is_pointskeleton
    #     )

    return (
        core_coords,
        core_vars,
        coords_needed,
    )


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


def _get_var_from_ds(
    var: str,
    aliases: dict[str, str],
    core: CoordinateManager,
    ds: xr.Dataset,
    decode_cf: bool,
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
    # 1) Use aliases mapping if exists
    if aliases.get(var) is not None:
        return aliases.get(var)

    # 2) Try to decode using cf standard name
    param = core.meta_parameter(var)
    if param is not None and decode_cf:
        ds_var = param.find_me_in_ds(ds)
        if len(ds_var) > 1:  # See if we have a perfect name match
            ds_var = [dv for dv in ds_var if param.name == ds_var]
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

    # 4) Reads a coordinate from a known list of aliases
    for alias_list in [
        LON_ALIASES,
        LAT_ALIASES,
        FREQ_ALIASES,
        DIRS_ALIASES,
        X_ALIASES,
        Y_ALIASES,
        TIME_ALIASES,
    ]:
        if var in alias_list:  # E.g. 'lon' in LON_ALIASES
            for alias_var in alias_list:  # E.g. LON_ALIASES = ['lon','lognitude']
                # E.g. 'longitude' in ds.data_vars
                if alias_var in ds.coords or alias_var in ds.data_vars:
                    return alias_var

    return None


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
