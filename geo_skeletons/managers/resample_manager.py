import pandas as pd
import geo_parameters as gp
import numpy as np
from scipy.stats import circmean
from typing import Union, Optional

def squared_mean(x, *args, **kwargs):
    """Calculates root mean of squares. Used for averaging significant wave height"""
    return np.sqrt(np.mean(x**2, *args, **kwargs))


def angular_mean(x, *args, **kwargs):
    """Calculates an angular mean for directions"""
    return circmean(x, *args, **kwargs)


def angular_mean_deg(x, *args, **kwargs):
    """Calculates an angular mean for directions with directions in degrees"""
    return np.rad2deg(circmean(np.deg2rad(x), *args, **kwargs))


def period_mean(x, *args, **kwargs):
    """Calculates an angular mean for wave periods (inverse of average of frequencies)"""
    return np.mean(x**-1.0, *args, **kwargs) ** -1.0


def set_up_mean_func(skeleton, var: str, new_dt: float, using_mag: bool=False) -> tuple:
    """Picks the right function to do the average and sets up a string to be set in the attributes"""
    if new_dt > 1:
        new_dt_str = f"{new_dt:.1f} h"
    else:
        new_dt_str = f"{new_dt*60:.0f} min"
    
    if using_mag:
        mean_func = None
        attr_str = f"{skeleton.dt()*60:.0f} min to {new_dt*60:.0f} min values through magnitude and direction"
    elif skeleton.meta.get(var).get("standard_name") == gp.wave.Hs.standard_name():
        mean_func = squared_mean
        attr_str = f"{skeleton.dt()*60:.0f} min to {new_dt*60:.0f} min values using np.sqrt(np.mean(x**2))"
    elif skeleton.meta.get(var).get("standard_name") is not None and (
        "wave_period" in skeleton.meta.get(var).get("standard_name")
        or "wave_mean_period" in skeleton.meta.get(var).get("standard_name")
    ):
        mean_func = period_mean
        attr_str = f"{skeleton.dt()*60:.0f} min to {new_dt_str} values using np.mean(x**-1.0)**-1.0"
    elif skeleton.core.get_dir_type(var) in ["from", "to"]:
        mean_func = angular_mean_deg
        attr_str = f"{skeleton.dt()*60:.0f} min to {new_dt_str} values using np.rad2deg(scipy.stats.circmean(np.deg2rad(x)))"
    elif skeleton.core.get_dir_type(var) == "math":
        attr_str = f"{skeleton.dt()*60:.0f} min to {new_dt_str} min values using scipy.stats.circmean(x)"
        mean_func = angular_mean
    else:
        mean_func = np.mean
        attr_str = (
            f"{skeleton.dt()*60:.0f} min to {new_dt*60:.0f} min values using np.mean"
        )
    return mean_func, attr_str


class ResampleManager:
    def __init__(self, skeleton):
        self.skeleton = skeleton

    def time(self, dt: Union[str, pd.Timedelta], dropna: bool = True, **kwargs):
        """Resamples all data in time"""
        coord_dict = self.skeleton.coord_dict()
        if "time" not in coord_dict.keys():
            raise ValueError("Skeleton does not have a time variable!")

        dt = pd.Timedelta(dt)/ pd.Timedelta('1 hour') # float in hours
        coord_dict["time"] = (
            self.skeleton.time(data_array=True).resample(time=f"{dt}h", **kwargs).mean().time
        )

        # Create new skeleton with hourly values
        new_skeleton = self.skeleton.from_coord_dict(coord_dict)
        new_data = {}

        data_vars_not_to_resample = []
        data_vars_to_resample = self.skeleton.core.data_vars()
        for key, val in self.skeleton.core._added_magnitudes.items():
            # If a magnitude and direction is defined, don't resample the components
            if val.direction is not None:
                data_vars_not_to_resample.append(val.x)
                data_vars_not_to_resample.append(val.y)
                # Resample the magnitude and direction instead
                data_vars_to_resample.append(key)
                data_vars_to_resample.append(val.direction.name)

        data_vars_to_resample = list(set(data_vars_to_resample) - set(data_vars_not_to_resample))
 
        for var in data_vars_to_resample:
            mean_func, attr_str = set_up_mean_func(
                self.skeleton, var, new_skeleton.dt()
            )

            new_skeleton.meta.append(
                {"resample_method": attr_str},
                var,
            )

            if var in self.skeleton.core.magnitudes():
                var_x = self.skeleton.core._added_magnitudes.get(var).x
                var_y = self.skeleton.core._added_magnitudes.get(var).y
                
                __, attr_str = set_up_mean_func(
                    self.skeleton, var_x, new_skeleton.dt(), using_mag=True
                )
                new_skeleton.meta.append(
                    {"resample_method": attr_str},
                    var_x,
                )
                __, attr_str = set_up_mean_func(
                    self.skeleton, var_y, new_skeleton.dt(), using_mag=True
                )
                new_skeleton.meta.append(
                    {"resample_method": attr_str},
                    var_y,
                )
            
            
            # Some version of python/xarray didn't like pd.Timedeltas in the resample method, so forcing to string
            new_data[var] = self.skeleton.get(var, data_array=True).resample(time=f"{dt}h",**kwargs).reduce(mean_func)

        for key, value in new_data.items():
            new_skeleton.set(key, value)

        new_skeleton = new_skeleton.from_ds(new_skeleton.ds().dropna(dim="time"), meta_dict=new_skeleton.meta.meta_dict())
        if not dropna:
            new_skeleton = new_skeleton.from_ds(
                new_skeleton.ds().resample(time=f"{dt}h",**kwargs).nearest(tolerance=f"{dt / 2}h"),  meta_dict=new_skeleton.meta.meta_dict()
            )
        return new_skeleton
