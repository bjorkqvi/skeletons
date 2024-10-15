import xarray as xr
import geo_parameters as gp
from typing import Union
from geo_parameters.metaparameter import MetaParameter


def set_core_vars_to_skeleton_from_ds(
    skeleton,
    ds: xr.Dataset,
    core_vars_to_ds_vars: dict[str, str],
    ds_remapped_coords: dict[str, list[str]],
    ds_dir_types: dict[str, str],
    meta_dict: dict = None,
):
    """Set core (static) variables to a skeleton from an xarray Dataset.

    Using the function 'geo_skeleton.decorders.identify_core_in_ds' we have gotten
    core_vars_to_ds_vars: dict mapping core variable to ds variable name
    coord_map: dict mapping variables of a core var ['time','inds','freq'] to variables of a ds var ['time','x','frequency']

    Optional:
    data_vars [default None]: list of ds_variables that will be set. All set if None.
    meta_dict: dict of core-var specific meta-data"""

    core_vars_to_ds_vars = core_vars_to_ds_vars or {}
    meta_dict = meta_dict or {}
    for var, ds_var in core_vars_to_ds_vars.items():
        if ds_remapped_coords.get(var):
            skeleton.set(
                var,
                ds.get(ds_var),
                coords=ds_remapped_coords[var],
                dir_type=ds_dir_types.get(ds_var),
            )
            old_metadata = {
                "standard_name": skeleton.meta.get(var).get("standard_name"),
                "units": skeleton.meta.get(var).get("units"),
            }
            skeleton.meta.append(ds.get(ds_var).attrs, name=var)
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


def add_dynamic_vars_from_ds(
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
        new_class = new_class.add_magnitude(**mag_dict)

    if new_class is None:
        new_class = skeleton_class
    return new_class
