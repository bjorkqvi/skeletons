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


def set_up_mean_func(skeleton, var: str, new_dt: float) -> tuple:
    """Picks the right function to do the average and sets up a string to be set in the attributes"""
    if new_dt > 1:
        new_dt_str = f"{new_dt:.1f} h"
    else:
        new_dt_str = f"{new_dt*60:.0f} min"
    
    
    if skeleton.meta.get(var).get("standard_name") == gp.wave.Hs.standard_name():
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

        dt = pd.Timedelta(dt)
        coord_dict["time"] = (
            self.skeleton.time(data_array=True).resample(time=dt, **kwargs).mean().time
        )

        # Create new skeleton with hourly values
        new_skeleton = self.skeleton.from_coord_dict(coord_dict)

        new_data = {}
        for var in self.skeleton.core.data_vars():
            mean_func, attr_str = set_up_mean_func(
                self.skeleton, var, new_skeleton.dt()
            )
            new_skeleton.meta.set(
                {"resample_method": attr_str},
                var,
            )
            # Make sure the angular values are given as math-directions for averaging
            new_data[var] = self.skeleton.ds()[var].resample(time=dt,**kwargs).reduce(mean_func)

        for key, value in new_data.items():
            new_skeleton.set(key, value)

        new_skeleton = new_skeleton.from_ds(new_skeleton.ds().dropna(dim="time"))
        if not dropna:
            new_skeleton = new_skeleton.from_ds(
                new_skeleton.ds().resample(time=dt, **kwargs).nearest(tolerance=dt / 2)
            )
        return new_skeleton
