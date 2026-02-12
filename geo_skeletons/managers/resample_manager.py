from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from geo_skeletons import Skeleton

import pandas as pd
import geo_parameters as gp
import numpy as np
from scipy.stats import circmean
from typing import Union, Optional
from .resample.scipy_regridders import scipy_regridders
from .resample.ravel import ravel_regridders
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
    if new_grid.proj.crs() is not None:
        new_data.proj.set(new_grid.proj.crs(), silent=True)
    new_data.name = data.name

    return new_data

REGRID_ENGINES = {'scipy': scipy_regridders, 'ravel': ravel_regridders}

class ResampleManager:
    def __init__(self, skeleton):
        self.skeleton = skeleton


    def engines(self):
        """Lists the available regridding engines and their supported regridding types.

        This method prints a detailed table of regridding engines, their availability, 
        supported regridding types, and installation requirements. It also lists the options 
        available for each engine.

        Supported regridding types include:
            - `grid-to-grid`: Regridding data from one grid to another.
            - `point-to-grid`: Regridding point data onto a grid.
            - `point-to-point`: Regridding or transforming point data to other points.
            - `grid-to-point`: Regridding grid data to point data.

        The information for each engine includes:
            - The engine name.
            - Whether it supports each regridding type.
            - Whether the engine is installed or available.
            - Installation instructions for unavailable engines.
            - Additional options supported by the engine.

        Args:
            None

        Returns:
            None: This method prints a table to the console.

        Notes:
            - The availability of engines is determined by the `REGRID_ENGINES` configuration.
            - Each engine may have specific installation requirements, which are also listed.
            - Use this method to assess which engines are installed and which regridding 
            types are supported for your use case.

        Example Output:
            -----------------------------------------------------------------------------------------------------------------------------
            Engine                  grid-to-grid    point-to-grid   point-to-point  grid-to-point   Installation
            -----------------------------------------------------------------------------------------------------------------------------
            'scipy' (Installed)          Yes              Yes             Yes            Yes        Native (default)
            'ravel' (Installed)           No               No             Yes            Yes        Native
            -----------------------------------------------------------------------------------------------------------------------------
            Engine          Options
            -----------------------------------------------------------------------------------------------------------------------------
            'scipy'         drop_nan: bool, mask_nan: float
            'ravel'         N/A
            -----------------------------------------------------------------------------------------------------------------------------
        """
        print('-'*125)
        print('Engine\t\t\tgrid-to-grid\tpoint-to-grid\tpoint-to-point\tgrid-to-point\tInstallation')
        print('-'*125)
        for key, value in REGRID_ENGINES.items():
            g2g = 'Yes' if value.get('gridded_to_gridded') is not None else ' No'
            p2g = 'Yes' if value.get('point_to_gridded') is not None else ' No'
            p2p = 'Yes' if value.get('point_to_point') is not None else ' No'
            g2p = 'Yes' if value.get('gridded_to_point') is not None else ' No'
            
            available = 'Installed' if value.get('available') else 'Not installed'

            print(f"'{key}' ({available})\t     {g2g}\t      {p2g}\t      {p2p}\t     {g2p}\t{value.get('installation')}")
        print('-'*125)
        print('Engine\t\tOptions')
        print('-'*125)
        for key, value in REGRID_ENGINES.items():
            
            print(f"'{key}'\t\t{value.get('options')}")
        print('-'*125)

    def grid(self, new_grid: Skeleton, target_class: Optional[Skeleton] = None, engine: str='scipy', verbose: bool=True, **kwargs) -> Skeleton:
        """Regrids the data of the Skeleton onto a new grid.

        This method regrids the data from the current Skeleton to a specified new grid using 
        the chosen regridding engine. The method will support multiple regridding engines and types 
        of regridding, and it can adapt to specific target classes if provided. The resulting 
        data is aligned with the new grid.

        Args:
            new_grid (Skeleton): The target grid to which the Skeleton data will be regridded. 
                This can be a grid-like object compatible with the regridding engine.
            target_class (Optional[Skeleton], optional): The target class for the regridded Skeleton. 
                If not provided, a new class is created automatically based on the new grid. 
                Defaults to `None`.
            engine (str, optional): The regridding engine to use. Must be one of the supported 
                engines in. Defaults to `'scipy'`. 
            verbose (bool, optional): If `True`, prints detailed information about the original 
                and target grids, as well as the regridding process. Defaults to `True`.
            **kwargs: Additional keyword arguments passed to the regridding engine.

        Returns:
            Skeleton: A new Skeleton instance regridded to the specified `new_grid`.

        Raises:
            ValueError: If the specified `engine` is not in the supported `REGRID_ENGINES`.
            NotImplementedError: If the specified regridding type is not available for the 
                chosen engine.

        Notes:
            - The method determines the type of regridding required (e.g., `'nearest'`, `'linear'`) 
            based on the original Skeleton and the new grid.
            - See .resample.engines() for allowed values of `engine` 
            - If `target_class` is not provided, a new class is automatically created to 
            match the structure of the new grid.

        Examples:
            Regrid the data to a new grid using the default `scipy` engine:
            >>> new_grid = create_new_grid(lon=(0, 10), lat=(45, 55))
            >>> regridded_data = skeleton.grid(new_grid)

            Regrid the data to a new grid using a specific target class:
            >>> regridded_data = skeleton.grid(new_grid, target_class=CustomSkeletonClass)
        """
        if engine not in REGRID_ENGINES.keys():
            raise ValueError(f"'engine' needs to be in {list(REGRID_ENGINES.keys())}, not '{engine}'!")

        regridder_dict = REGRID_ENGINES.get(engine)
        regrid_type = sort_out_regridded_type(self.skeleton, new_grid)

        regridder = regridder_dict.get(regrid_type)

        if regridder is None:
            raise NotImplementedError(f"'{regrid_type}' regridding not available for engine '{engine}'")

        new_class = target_class or create_new_class(self.skeleton, new_grid)
        new_data = init_new_class_to_grid(new_class, new_grid, self.skeleton)

        if verbose:
            if self.skeleton.core.is_projected():
                print(f"Original data has spatial coords {self.skeleton.core.coords('spatial')} CRS {str(self.skeleton.proj.crs())[:20]}")
            else:
                print(f"Original data has spatial coords {self.skeleton.core.coords('spatial')}")
            if new_data.core.is_projected():
                print(f"Target grid has spatial coords {new_data.core.coords('spatial')} CRS {str(new_data.proj.crs())[:20]}")
            else:
                print(f"Target grid has spatial coords {new_data.core.coords('spatial')}")

            
            print(f"Starting regridding ('{regrid_type}') with '{engine}'({regridder})...")
        
        new_data = regridder(self.skeleton, new_grid, new_data, verbose=verbose, **kwargs)
        
        return new_data

    def time(
        self,
        dt: Union[str, pd.Timedelta],
        dropna: bool = False,
        mode: str = "left",
        skipna: bool = False,
        all_times: bool = False,
    ) -> Skeleton:
        """Resamples the data of the Skeleton in time.

        This method resamples the data based on the specified time step (`dt`) and calculates 
        averages or aggregates based on the type of variable. It supports handling NaN values 
        and provides options for missing time stamps and different averaging modes.

        Args:
            dt (Union[str, pd.Timedelta]): The new time step for resampling. Examples:
                - A string, such as `'30min'` or `'3h'`.
                - A `pandas.Timedelta` object, such as `pd.Timedelta(hours=6)`.
            dropna (bool, optional): If `True`, drops rows with NaN values after resampling. 
                Defaults to `False`.
            mode (str, optional): Specifies the type of averaging to perform. Must be one of:
                - `'left'` (default): Averages values aligned to the start of the time window.
                - `'right'`: Averages values aligned to the end of the time window.
                - `'centered'`: Averages values centered within the time window.
            skipna (bool, optional): If `True`, skips NaN values in the original data when 
                calculating averages. Defaults to `False`.
            all_times (bool, optional): If `True`, includes NaN values for missing time stamps 
                in the resampled data. Defaults to `False`.

        Returns:
            Skeleton: A new Skeleton instance with resampled time data.

        Notes:
            - Significant wave height (`geo_parameters.wave.Hs`) is averaged using the formula:
            `np.sqrt(np.mean(hs**2))`.
            - Circular variables (those with a `dir_type`) are averaged using `scipy.stats.circmean`.
            - Wave periods are averaged through their frequency: `np.mean(Tp**-1.0)**-1.0`.
            - For Skeleton magnitudes and directions, the resampled components are determined 
            using the resampled magnitude and direction.
            - Max-parameters (e.g., `geo_parameters.wave.Hmax` and `EtaMax`) are resampled 
            using the maximum value: `np.max`.

        Examples:
            Resample 10-minute data from 2020-01-01 00:00 to 2020-01-01 01:00 with values `[0, 1, 2, 3, 4, 5, 6]`:

            Example 1: Resample with 30-minute intervals (`mode='left'`, default):
            >>> resample.time(dt="30min")
            times: ['2020-01-01 00:00', '2020-01-01 00:30', '2020-01-01 01:00']
            values: [1, 4, 6]

            Example 2: Resample with 30-minute intervals and `mode='right'`:
            >>> resample.time(dt="30min", mode='right')
            times: ['2020-01-01 00:00', '2020-01-01 00:30', '2020-01-01 01:00']
            values: [0, 2, 5]

            Example 3: Resample with 30-minute intervals and `mode='centered'`:
            >>> resample.time(dt="30min", mode='centered')
            times: ['2020-01-01 00:00', '2020-01-01 00:30', '2020-01-01 01:00']
            values: [0.5, 3, 5.5]
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
