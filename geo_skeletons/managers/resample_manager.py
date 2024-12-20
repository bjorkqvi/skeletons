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


def set_up_mean_func(
    skeleton, var: str, new_dt: float, mode: str, using_mag: bool = False
) -> tuple:
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
    elif skeleton.meta.get(var).get("standard_name") is not None and (
        "maximum" in skeleton.meta.get(var).get("standard_name")
        and "height" in skeleton.meta.get(var).get("standard_name")
    ):
        mean_func = np.max
        attr_str = f"{skeleton.dt()*60:.0f} min to {new_dt_str} values using np.max(x)"
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

    attr_str = f"{mode} mean {attr_str}"
    return mean_func, attr_str


class ResampleManager:
    def __init__(self, skeleton):
        self.skeleton = skeleton

    def time(
        self,
        dt: Union[str, pd.Timedelta],
        dropna: bool = False,
        mode: str = "left",
        skipna: bool = False,
        all_times: bool = False,
    ):
        """Resamples the data of the Skeleton in time.

        dt is new time step: '30min', '3h', pd.Timedelta(hours=6)
        dropna [default False]: Drop NaN values
        mode ('start' [default], 'end', or 'centered'): Type of average being calculated
        skipna [default False]: skips NaN values in the original data when calculating the mean values
        all_times [default False]: Create NaN values for miossing time stamps

        - Significant wave height (geo_parameters.wave.Hs) will be averaged using np.sqrt(np.mean(hs**2))
        - Circular variables (those having a dir_type) will be averaged using scipy.stats.circmean
        - Wave periods will be averaged through the frequency: np.mean(Tp**-1.0)**-1.0
        - For Skeleton Magnitude and direction, the resampled components will be determined using the resampled of magnitude and direction
        - Max-paramters (e.g. geo_parameters.wave.Hmax and EtaMax) will be resampled as np.max

        Example: 10min values 2020-01-01 00:00 to 2020-01-01 01:00, val = [0,1,2,3,4,5,6]

        Ex1: resample.time(dt="30min")
            times: ['2020-01-01 00:00', '2020-01-01 00:30', '2020-01-01 01:00']
            values: [1,4,6]

        Ex2: resample.time(dt="30min", mode='right')
            times: ['2020-01-01 00:00', '2020-01-01 00:30', '2020-01-01 01:00']
            values: [0,2,5]

        Ex2: resample.time(dt="30min", mode='centered')
            times: ['2020-01-01 00:00', '2020-01-01 00:30', '2020-01-01 01:00']
            values: [0.5,3,5.5]
        """
        coord_dict = self.skeleton.coord_dict()
        if "time" not in coord_dict.keys():
            raise ValueError("Skeleton does not have a time variable!")

        dt = pd.Timedelta(dt) / pd.Timedelta("1 hour")  # float in hours

        if mode == "left":
            closed = "left"
            label = "left"
        elif mode == "right":
            closed = "right"
            label = "right"
        elif mode == "centered":
            closed = "right"
            label = None
        else:
            raise ValueError(f"'mode' must be 'left', 'right' or 'centered'!")

        coord_dict["time"] = (
            self.skeleton.time(data_array=True)
            .resample(time=f"{dt}h", closed=closed, skipna=skipna, label=label)
            .mean()
            .time
        )

        # Create new skeleton with hourly values
        new_skeleton = self.skeleton.from_coord_dict(coord_dict)
        new_skeleton.meta.set_by_dict({"_global_": self.skeleton.meta.get()})

        new_data = {}

        if mode == "left":
            offset = pd.Timedelta(hours=0)
        elif mode == "right":
            offset = pd.Timedelta(hours=0)
        elif mode == "centered":
            if np.isclose(new_skeleton.dt() / self.skeleton.dt() % 2, 0):
                raise ValueError(
                    f"When using centered mean, the new time step {new_skeleton.dt()} must be and odd multiple of the old timestep {self.skeleton.dt()}!"
                )
            offset = pd.Timedelta(hours=new_skeleton.dt() / 2)

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

        data_vars_to_resample = list(
            set(data_vars_to_resample) - set(data_vars_not_to_resample)
        )

        for var in data_vars_to_resample:
            mean_func, attr_str = set_up_mean_func(
                self.skeleton, var, new_skeleton.dt(), mode
            )

            new_skeleton.meta.append(
                {"resample_method": attr_str},
                var,
            )

            if var in self.skeleton.core.magnitudes():
                var_x = self.skeleton.core._added_magnitudes.get(var).x
                var_y = self.skeleton.core._added_magnitudes.get(var).y

                __, attr_str = set_up_mean_func(
                    self.skeleton, var_x, new_skeleton.dt(), mode, using_mag=True
                )
                new_skeleton.meta.append(
                    {"resample_method": attr_str},
                    var_x,
                )
                __, attr_str = set_up_mean_func(
                    self.skeleton, var_y, new_skeleton.dt(), mode, using_mag=True
                )
                new_skeleton.meta.append(
                    {"resample_method": attr_str},
                    var_y,
                )

            # Some version of python/xarray didn't like pd.Timedeltas in the resample method, so forcing to string
            new_data[var] = (
                self.skeleton.get(var, data_array=True)
                .resample(time=f"{dt}h", closed=closed, offset=offset, skipna=skipna)
                .reduce(mean_func)
            )

        for key, value in new_data.items():
            new_skeleton.set(key, value)

        if dropna:
            new_skeleton = new_skeleton.from_ds(
                new_skeleton.ds().dropna(dim="time"),
                meta_dict=new_skeleton.meta.meta_dict(),
                keep_ds_names=True,
                decode_cf=False,
            )
        elif all_times:
            new_skeleton = new_skeleton.from_ds(
                new_skeleton.ds()
                .resample(time=f"{dt}h")
                .nearest(tolerance=f"{dt / 2}h"),
                meta_dict=new_skeleton.meta.meta_dict(),
                keep_ds_names=True,
                decode_cf=False,
            )
        return new_skeleton
