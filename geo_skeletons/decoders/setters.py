import xarray as xr
from .ds_decoders import find_settable_vars_and_magnitudes

import geo_parameters as gp


def set_core_vars_to_skeleton_from_ds(
    skeleton,
    ds: xr.Dataset,
    core_vars: dict,
    coord_map: dict,
    meta_dict: dict = None,
    data_vars: list[str] = None,
    ignore_vars: list[str] = None,
):
    """Set core (static) variables to a skeleton from an xarray Dataset.

    Using the function 'geo_skeleton.decorders.identify_core_in_ds' we have gotten
    core_vars: dict mapping core variable to ds variable name
    coord_map: dict mapping variables of a core var ['time','inds','freq'] to variables of a ds var ['time','x','frequency']

    Optional:
    data_vars [default None]: list of ds_variables that will be set. All set if None.
    meta_dict: dict of core-var specific meta-data"""

    data_vars = data_vars or []
    ignore_vars = ignore_vars or []
    meta_dict = meta_dict or {}
    for var, ds_var in core_vars.items():
        if (
            not data_vars or ds_var in data_vars
        ) and not ds_var in ignore_vars:  # If list is specified, only add those variables
            skeleton.set(var, ds.get(ds_var), coords=coord_map[var])
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
    ds: xr.Dataset,
    settable_vars,
    settable_magnitudes,
):
    """Find all the variables in the Dataset that can e added to the skeleton_class.

    Reasons not to add:
    1) It already exists (based on name or standard_name of geo-parameter)
    2) We can't find a coordinate groupt that fits the dimensions"""

    core_vars = {}
    coord_map = {}
    new_class = None
    for ds_var, (var, coord_group, coords) in settable_vars.items():
        var_str, __ = gp.decode(var)
        core_vars[var_str] = ds_var
        coord_map[var_str] = coords

        if new_class is None:
            new_class = skeleton_class.add_datavar(var, coord_group=coord_group)
        else:
            new_class = new_class.add_datavar(var, coord_group=coord_group)

    for mag, dirs in settable_magnitudes:
        x = new_class.core.find_cf(mag.my_family().get("x").standard_name())[0]
        y = new_class.core.find_cf(mag.my_family().get("y").standard_name())[0]
        new_class = new_class.add_magnitude(mag, x=x, y=y, direction=dirs)

    if new_class is None:
        new_class = skeleton_class
    return new_class, core_vars, coord_map
