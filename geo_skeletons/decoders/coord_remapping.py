import xarray as xr
from geo_skeletons.managers.coordinate_manager import CoordinateManager
from copy import deepcopy
from typing import Union
from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp


def remap_coords_of_ds_vars_to_skeleton_names(
    ds: xr.Dataset,
    core: CoordinateManager,
    core_vars_to_ds_vars: dict[str, str],
    core_coords: dict[str, str],
    core_lens: dict[str, int],
    addable_vars: list[Union[MetaParameter, str]] = None,
    addable_magnitudes: list[dict[str, MetaParameter]] = None,
    conservative_expansion: bool = False,
) -> tuple[dict, dict]:
    """Remaps the coordinates of a Dataset variable to the Skeleton variables of a pre-defined coord_group"""
    remapped_coords = {}
    ds_coord_groups = {}
    core_vars_to_ds_vars = core_vars_to_ds_vars or {}
    addable_vars = addable_vars or []
    addable_magnitudes = addable_magnitudes or {}

    for var, ds_var in core_vars_to_ds_vars.items():
        if isinstance(ds_var, tuple):
            ds_var = ds_var[0]
        if ds.get(ds_var) is None:
            continue
        ds_coords = list(ds.get(ds_var).dims)

        ds_lens = [len(ds.get(c)) for c in ds_coords]

        remapped_coord, coord_group = _remap_ds_coords(
            ds_coords=ds_coords,
            ds_lens=ds_lens,
            core=core,
            core_coords=core_coords,
            core_lens=core_lens,
            conservative_expansion=conservative_expansion,
        )

        if remapped_coord is not None:
            remapped_coords[var], ds_coord_groups[var] = (
                remapped_coord,
                coord_group,
            )
    # Fill in coordinate croups of variables that are added but not in the Dataset
    # E.g. If wind and wind direction in Dataset, then wind_x and wind_y will be added
    mags = [mag_dict.get("name") for mag_dict in addable_magnitudes]
    dirs = [mag_dict.get("direction") for mag_dict in addable_magnitudes]
    for var in addable_vars:
        var_str, var = gp.decode(var)
        if var is not None and var_str not in core_vars_to_ds_vars.keys():
            if var.i_am() in ["x", "y"]:
                mag = var.my_family("magnitude").find_me_in(mags, return_first=True)
                remapped_coords[var_str] = remapped_coords.get(mag.name)
                ds_coord_groups[var_str] = ds_coord_groups.get(mag.name)

    return remapped_coords, ds_coord_groups


def _remap_ds_coords(
    ds_coords: list[str],
    ds_lens: list[int],
    core: CoordinateManager,
    core_coords: dict[str, str],
    core_lens: dict[str, int],
    conservative_expansion: bool,
):

    coord_group = None
    cartesian = core_coords.get("x") is not None

    if conservative_expansion:
        cg_list = ["gridpoint", "grid", "all", "spatial"]
    else:
        cg_list = ["grid", "all", "gridpoint", "spatial"]

    for cg in cg_list:
        skeleton_coords = _get_skeleton_goord_group_coords(
            core.coords(cg), cartesian=cartesian
        )

        coords, unused_core_coords, unused_core_coords_len = _rename_coords(
            ds_coords=ds_coords,
            core_coords=core_coords,
            coords_needed=skeleton_coords,
            core_lens=core_lens,
        )

        if None in coords:
            coords = _patch_unknown_coords(
                coords,
                unused_core_coords,
                unused_core_coords_len,
                ds_lens=ds_lens,
            )

        # # Couldn't find a match for all non-trivial coordinates
        if None in coords:
            continue

        # The remapped coords need to cover all non-trivial dimensions in the skeleton coord group
        coords_not_covered = set(skeleton_coords) - set(coords)
        lens_not_covered = [core_lens.get(c) for c in coords_not_covered]

        if not lens_not_covered or max(lens_not_covered) < 2:
            break

    if None in coords or cg is None:
        return None, None
    return coords, cg


def _get_skeleton_goord_group_coords(skeleton_coords: list[str], cartesian: bool):
    """Switch out x/y <=> lon/lat if needed"""
    if cartesian:
        skeleton_coords = list(map(lambda x: x.replace("lat", "y"), skeleton_coords))
        skeleton_coords = list(map(lambda x: x.replace("lon", "x"), skeleton_coords))
    else:
        skeleton_coords = list(map(lambda x: x.replace("y", "lat"), skeleton_coords))
        skeleton_coords = list(map(lambda x: x.replace("x", "lon"), skeleton_coords))

    return skeleton_coords


def _patch_unknown_coords(
    coords: list[str],
    unused_core_coords: list[str],
    unused_core_coords_len: list[int],
    ds_lens: list[int],
) -> list[str]:
    """Try to fill in coords that were not known. Removes trivial dimensions."""

    for n, coord in enumerate(coords):
        if coord is None:
            inds_of_matching_core_lens = [
                i for i, j in enumerate(unused_core_coords_len) if j == ds_lens[n]
            ]

            if ds_lens[n] > 1:  # Non-trivial coordinate
                # We can only match if we have exactcly one coordinate of matching length
                if len(inds_of_matching_core_lens) == 1:
                    coords[n] = unused_core_coords[inds_of_matching_core_lens[0]]
                else:
                    coords[n] = None
            elif ds_lens[n] == 1:  # Trivial coordinate
                # Trivial coordinates are not needed for setting
                # But try to get e.g. 'inds' right so we can find a coord_group
                if len(inds_of_matching_core_lens) == 1:
                    coords[n] = unused_core_coords[inds_of_matching_core_lens[0]]
                else:
                    coords[n] = "REMOVETHISCOORD"
            else:
                raise ValueError(
                    f"Length of Dataset coordinate needs to be positive, not{ds_lens[n]}!"
                )

    coords = [c for c in coords if c != "REMOVETHISCOORD"]

    return coords


def _rename_coords(
    ds_coords: list[str],
    core_coords: dict,
    coords_needed: list[str],
    core_lens: dict[str, int],
) -> list[str]:
    """Maps the coordinates of a single Dataarray to the coordinates of the core variable. Needed be

    E.g.
    - core variable 'hs' defined over 'time', 'inds', 'freq'
    - Matching ds variable defined over 'station', 'time', 'frequency'

    function returns ['inds', 'time', 'freq']
    Ther can be used to set data to the Skeleton without explicitly reshaping, since the dims in the Dataset have worng names
    """

    unused_core_coords = []
    unused_core_coords_len = []

    # Need to rename the coordinates so they can be used in the reshape
    reversed_dict = {}
    for key, value in core_coords.items():
        reversed_dict[value] = key
    coords = []

    for n, ds_c in enumerate(ds_coords):
        core_c = reversed_dict.get(ds_c)
        if core_c in coords_needed:
            coords.append(core_c)
        else:
            coords.append(None)

    for coord in coords_needed:
        if not coord in coords:
            unused_core_coords.append(coord)
            unused_core_coords_len.append(core_lens.get(coord))

    return coords, unused_core_coords, unused_core_coords_len


#     # # Data can be given as x-y with trivial y for example
#     # if "inds" not in coords and is_pointskeleton:
#     #     for n, ds_c in missed_coords:
#     #         if len(ds.get(ds_c)) > 1 or max_len_of_missed_coords == 1:
#     #             coords[n] = "inds"
#     # coords = [c for c in coords if c is not None]
#     # return coords
