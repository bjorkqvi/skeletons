from __future__ import annotations
import xarray as xr
import geo_parameters as gp
from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np
from geo_parameters.metaparameter import MetaParameter
from .core_decoders import identify_core_in_ds, gather_coord_values
from .ds_decoders import map_ds_to_gp, find_addable_vars_and_magnitudes
from .coord_remapping import remap_coords_of_ds_vars_to_skeleton_names


def create_new_class_dynamically(
    cls,
    ds: xr.Dataset,
    ignore_vars: list[str],
    only_vars: list[str],
    keep_ds_names: bool,
    decode_cf: bool,
    core_aliases: dict[Union[str, MetaParameter], str],
    ds_aliases: dict[str, Union[MetaParameter, str]],
    extra_coords: dict[str, Union[np.ndarray, xr.DataArray]],
    verbose: bool,
):
    """Creates a new Skeleton class (modified from provided class) that contains the variables matching the xr.Dataset"""
    (
        core_coords_to_ds_coords,
        core_vars_to_ds_vars,
        coords_needed,
    ) = identify_core_in_ds(
        cls.core,
        ds,
        aliases=core_aliases,
        ignore_vars=ignore_vars,
        only_vars=only_vars,
        allowed_misses=list(extra_coords.keys()),
    )

    coords = gather_coord_values(
        coords_needed, ds, core_coords_to_ds_coords, extra_coords=extra_coords
    )
    points = cls(**coords)
    # Lengths needed for matching coordinates with wrong name
    # We do this instead of reading the lengths of the arrays directyl
    # Reason is that we want 'inds' for PointSkeletons etc
    core_lens = {c: len(points.get(c)) for c in points.core.coords("all")}

    # Map every variable in the Dataset to a MetaParameter if possible
    ds_vars_to_gp, __ = map_ds_to_gp(
        ds,
        decode_cf=decode_cf,
        keep_ds_names=keep_ds_names,
        aliases=ds_aliases,
        verbose=verbose,
    )

    # Find which ones of the Dataset variables should be added
    # Also determine if there are some Magnitude-Direction pairs that should be added
    addable_vars, addable_magnitudes, new_core_vars_to_ds_vars = (
        find_addable_vars_and_magnitudes(
            core=cls.core,
            ds_vars_to_gp=ds_vars_to_gp,
            core_vars_to_ds_vars=core_vars_to_ds_vars,
            only_vars=only_vars,
            ignore_vars=ignore_vars,
        )
    )
    # Add new coords in here to they represent the new class that we will create
    core_vars_to_ds_vars.update(new_core_vars_to_ds_vars)

    # Find matching coordinate groups that we need to add the variables to the class
    __, ds_coord_groups = remap_coords_of_ds_vars_to_skeleton_names(
        ds,
        cls.core,
        core_vars_to_ds_vars,
        core_coords_to_ds_coords,
        core_lens,
        addable_vars,
        addable_magnitudes,
    )

    # Create the new class
    cls = _add_dynamic_vars_from_ds(
        cls,
        addable_vars,
        addable_magnitudes,
        ds_coord_groups,
    )
    return cls


def set_core_vars_to_skeleton_from_ds(
    skeleton,
    ds: xr.Dataset,
    core_vars_to_ds_vars: dict[str, str],
    ds_remapped_coords: dict[str, list[str]],
    meta_dict: dict = None,
):
    """Set core (static) variables to a skeleton from an xarray Dataset.

    Using the function 'geo_skeleton.decorders.identify_core_in_ds' we have gotten
    core_vars_to_ds_vars: dict mapping core variable to ds variable name
    ds_remapped_coords: dict mapping variables of a core var ['time','inds','freq'] to variables of a ds var ['time','x','frequency']

    Optional:
    meta_dict: dict of core-var specific meta-data"""

    core_vars_to_ds_vars = core_vars_to_ds_vars or {}
    meta_dict = meta_dict or {}

    for var, ds_var_x in core_vars_to_ds_vars.items():
        get_metadata_from_ds = True
        if isinstance(ds_var_x, tuple):
            ds_var_x, ds_var_y, transform_function, dir_type = ds_var_x
            data_to_set = transform_function(ds.get(ds_var_x), ds.get(ds_var_y))
            if ds_var_y is None:
                get_metadata_from_ds = False
        else:
            data_to_set = ds.get(ds_var_x)
            dir_type = None

        if ds_remapped_coords.get(var):
            skeleton.set(
                var,
                data_to_set,
                coords=ds_remapped_coords[var],
                dir_type=dir_type,
            )
            old_metadata = {
                "standard_name": skeleton.meta.get(var).get("standard_name"),
                "units": skeleton.meta.get(var).get("units"),
            }
            if get_metadata_from_ds:
                skeleton.meta.append(ds.get(ds_var_x).attrs, name=var)
            skeleton.meta.append(old_metadata, name=var)
            skeleton.meta.append(meta_dict.get(var, {}), name=var)

    for var in skeleton.core.magnitudes():
        old_metadata = {
            "standard_name": skeleton.meta.get(var).get("standard_name"),
            "units": skeleton.meta.get(var).get("units"),
        }
        skeleton.meta.append(old_metadata, name=var)
        skeleton.meta.append(meta_dict.get(var, {}), name=var)

    for var in skeleton.core.directions():
        old_metadata = {
            "standard_name": skeleton.meta.get(var).get("standard_name"),
            "units": skeleton.meta.get(var).get("units"),
        }
        skeleton.meta.append(old_metadata, name=var)
        skeleton.meta.append(meta_dict.get(var, {}), name=var)

    return skeleton


def _add_dynamic_vars_from_ds(
    skeleton_class,
    addable_vars: list[Union[MetaParameter, str]],
    addable_magnitudes: list[dict[str, MetaParameter]],
    ds_coord_groups: dict[str, str],
):
    """Add data variables, magnitudes and direction."""
    new_class = None
    for var in addable_vars:
        var_str, var = gp.decode(var)
        if ds_coord_groups.get(var_str):
            if new_class is None:
                new_class = skeleton_class.add_datavar(
                    var or var_str, coord_group=ds_coord_groups[var_str]
                )
            else:
                new_class = new_class.add_datavar(
                    var or var_str, coord_group=ds_coord_groups[var_str]
                )

    for mag_dict in addable_magnitudes:
        # x = new_class.core.find_cf(mag.my_family().get("x").standard_name())[0]
        # y = new_class.core.find_cf(mag.my_family().get("y").standard_name())[0]
        if new_class is None:
            new_class = skeleton_class.add_magnitude(**mag_dict)
        else:
            new_class = new_class.add_magnitude(**mag_dict)

    if new_class is None:
        new_class = skeleton_class
    return new_class
