import pandas as pd
import geo_parameters as gp
import numpy as np
from scipy.stats import circmean
from typing import Union, Optional
from .resample.scipy_regridders import scipy_regridders
import geo_parameters as gp
from copy import deepcopy
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

def find_original_skeleton_in_inheritance_chain(data):
    cls = data.__class__
    chain = [cls]
    while cls.__bases__:  # While there are more parents in the hierarchy
        cls = cls.__bases__[0]
        chain.append(cls)
    return chain[-3]

def sort_out_regridded_type(data, new_grid) -> str:
    """Determines if we need to do gridded-gridded, point-gridded etc regridding"""
    source = 'gridded' if data.is_gridded() else 'point'
    target = 'gridded' if new_grid.is_gridded() else 'point'
    return f"{source}_to_{target}"


def create_new_class(data, new_grid):
    """Creates a new class that will contain the gridded data. 
    If we change type (i.e. from gridded to point), then we will reconstruct the class to contain the correct data variables etc"""
    if data.is_gridded() and new_grid.is_gridded():
        return data.__class__
    if not data.is_gridded() and not new_grid.is_gridded():
        return data.__class__

    
    old_base = find_original_skeleton_in_inheritance_chain(new_grid)
    if new_grid.is_gridded():
        new_base = type(f'Gridded{data.__class__.__name__}', (old_base,), {})
    else:
        new_base = type(f'Point{data.__class__.__name__}', (old_base,), {})

    for key, param in data.core._added_coords.items():
        if key == 'time':
            new_base=new_base.add_time()
        elif key in ['x','y','lon','lat','inds']:
            continue
        elif gp.wave.Freq.is_same(param.meta):
            new_base = new_base.add_frequency(param)
        elif gp.wave.Dirs.is_same(param.meta):
            new_base = new_base.add_direction(param)
        else:   
            new_base = new_base.add_coord(param)

    for key, param in data.core._added_vars.items():
        if key not in ['x','y','lon','lat']:
            new_base = new_base.add_datavar(param)
    
    for key, param in data.core._added_magnitudes.items():
        direction = param.direction
        if direction is not None:
            dir_type = direction.dir_type
            direction = direction.meta or direction.name
        else:
            direction, dir_type = None, None
        new_base = new_base.add_magnitude(param.meta or param.name, x=param.x, y=param.y, direction=direction, dir_type=dir_type)
           
    ignore_these = []
    for key, param in data.core._added_masks.items():
        opposite_mask = param.opposite_mask
        if opposite_mask is not None:
            opposite_name = opposite_mask.meta or opposite_mask.name[:-5]
            ignore_these.append(opposite_name)
        else:
            opposite_name = None
        if (param.meta or param.name[:-5]) not in ignore_these:
            new_base = new_base.add_mask(param.meta or param.name[:-5], default_value=param.default_value, coord_group=param.coord_group, opposite_name=opposite_name, triggered_by=param.triggered_by, valid_range=param.valid_range, range_inclusive=param.range_inclusive)
    

    new_base_coords = new_base.core._added_coords
    new_base_vars = new_base.core._added_vars
    new_base.core = deepcopy(data.__class__.core) # Copy over coordinates, data variables, magnitudes, masks etc.
    if not data.is_gridded() and new_grid.is_gridded():
        del new_base.core._added_coords['inds']
        del new_base.core._added_vars['x']
        del new_base.core._added_vars['y']
        new_base.core._added_coords['x'] = new_base_coords['x']
        new_base.core._added_coords['y'] = new_base_coords['y']

        
        return new_base

    if data.is_gridded() and not new_grid.is_gridded():
        del new_base.core._added_coords['x']
        del new_base.core._added_coords['y']

        new_base.core._added_coords['inds'] = new_base_coords['inds']
        new_base.core._added_vars['x'] = new_base_vars['x']
        new_base.core._added_vars['y'] = new_base_vars['y']

        
        return new_base


def init_new_class_to_grid(new_class, new_grid, data):
    """Initializes new class to the wanted grid
    Other coordinates (such as time) are copied over from original data"""
    # This is a hack. Make it better later
    new_lon, new_lat = new_grid.lon(native=True), new_grid.lat(native=True)
    new_coords = {new_grid.core.x_str: new_lon, new_grid.core.y_str: new_lat}
    
    for coord in data.core.coords():
        if coord not in ['x','y','lon','lat','inds']:
            new_coords[coord] = data.get(coord)
       
    new_data = new_class(**new_coords)
    if new_data.core.is_cartesian():
        new_data.utm.set(data.utm.zone(), silent=True)

    return new_data

REGRID_ENGINES = {'scipy': scipy_regridders}

class ResampleManager:
    def __init__(self, skeleton):
        self.skeleton = skeleton


    def grid(self, new_grid, engine: str='scipy'):
        """Regrids the data of the skeleton to a new grid"""
        if engine not in REGRID_ENGINES.keys():
            raise ValueError(f"'engine' needs to be in {list(REGRID_ENGINES.keys())}, not '{engine}'!")
        
        new_cls = create_new_class(self.skeleton, new_grid)
        new_data = init_new_class_to_grid(new_class, new_grid, self.skeleton)

        regridder_dict = REGRID_ENGINES.get(engine)
        regrid_type = sort_out_regridded_type(self.skeleton, new_grid)

        regridder = regridder_dict.get(regrid_type)

        if regridder is None:
            raise NotImplementedError(f"'{regrid_type}' regridding not available for engine '{engine}'")

        new_data = regridder(self.skeleton, new_grid, new_data)
        
        return new_data

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
