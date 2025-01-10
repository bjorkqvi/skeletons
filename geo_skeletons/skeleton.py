import numpy as np
import xarray as xr
from .managers.dataset_manager import DatasetManager
from .managers.dask_manager import DaskManager
from .managers.reshape_manager import ReshapeManager
from .managers.resample_manager import ResampleManager
from .decoders import (
    identify_core_in_ds,
    set_core_vars_to_skeleton_from_ds,
    create_new_class_dynamically,
    find_addable_vars_and_magnitudes,
    map_ds_to_gp,
    remap_coords_of_ds_vars_to_skeleton_names,
    gather_coord_values,
)
from . import data_sanitizer as sanitize
from .managers.utm_manager import UTMManager
from typing import Iterable, Union, Optional
from . import distance_funcs
from .errors import (
    DataWrongDimensionError,
    DirTypeError,
    SkeletonError,
    UnknownVariableError,
)

from typing import Iterable
from copy import deepcopy
from .decorators import (
    add_datavar,
    add_magnitude,
    add_mask,
    add_time,
    add_frequency,
    add_direction,
    add_coord,
)
from .iter import SkeletonIterator

from geo_skeletons import dask_computations, dir_conversions
import itertools

import geo_parameters as gp
from geo_parameters.metaparameter import MetaParameter

from .distance_funcs import distance_2points
import pandas as pd
from copy import deepcopy


class Skeleton:
    """Contains methods and data of the spatial x,y / lon, lat coordinates and
    makes possible conversions between them.

    Keeps track of the native structure of the grid (cartesian UTM / sperical).
    """

    chunks = None

    def __init__(
        self,
        x: Optional[Union[Iterable[float], Iterable[int], float, int]] = None,
        y: Optional[Union[Iterable[float], Iterable[int], float, int]] = None,
        lon: Optional[Union[Iterable[float], Iterable[int], float, int]] = None,
        lat: Optional[Union[Iterable[float], Iterable[int], float, int]] = None,
        name: str = "LonelySkeleton",
        utm: Optional[tuple[int, str]] = None,
        chunks: Union[tuple[int], str] = None,
        **kwargs,
    ) -> None:
        self.name = name
        self._init_structure(x, y, lon, lat, **kwargs)
        self._init_managers(utm=utm, chunks=chunks)
        self._init_metadata()

    def _init_structure(self, x=None, y=None, lon=None, lat=None, **kwargs) -> None:
        """Determines grid type (Cartesian/Spherical), generates a DatasetManager
        and initializes the Xarray dataset within the DatasetManager.

        The initial coordinates and variables are read from the method of the
        subclass (e.g. PointSkeleton)
        """

        # Don't want to alter the CoordManager of the class
        if not self.core._is_initialized():
            self.core = deepcopy(self.core)  # Makes a copy of the class coord_manager
            self.meta = self.core.meta

        # # The manager will contain the Xarray Dataset
        if self.ds() is None:
            self._ds_manager = DatasetManager(self.core)
        else:
            self._ds_manager.coord_manager = self.core
        self.meta._ds_manager = self._ds_manager

        x, y, lon, lat, kwargs = sanitize.sanitize_input(
            x, y, lon, lat, self.is_gridded(), **kwargs
        )

        x_str, y_str, xvec, yvec = sanitize.will_grid_be_spherical_or_cartesian(
            x, y, lon, lat
        )
        self.core.x_str = x_str
        self.core.y_str = y_str

        # Reset initial coordinates and data variables (default are 'x','y' but might now be 'lon', 'lat')
        self.core.set_initial_coords(self._initial_coords(spherical=(x_str == "lon")))
        self.core.set_initial_vars(self._initial_vars(spherical=(x_str == "lon")))

        self._ds_manager.create_structure(x=xvec, y=yvec, new_coords=kwargs)

    def _init_managers(self, utm: tuple[str, int], chunks: tuple[int]) -> None:
        """Initialized a DirTypeManager, UTMManager and DaskManager, and sets a UTM-zone"""
        if chunks is None:
            if hasattr(self, "_chunks"):  # Set by @activate_dask-decorator
                chunks = self._chunks

        self.dask = DaskManager(skeleton=self, chunks=chunks)

        self.utm = UTMManager(
            lat=self.edges("lat", strict=True),
            lon=self.edges("lon", strict=True),
            metadata_manager=self.meta,
        )
        self.utm.set(utm, silent=True)
        self.resample = ResampleManager(self)

    def _init_metadata(self) -> None:
        """Initialized the metadata by using availabe metadata in the GeoParameters"""
        for name in self.core.all_objects("all"):
            metavar = self.core.get(name).meta
            if metavar is not None:
                self.meta.append(metavar.meta_dict(), name)

        self.meta.append({"name": self.name})

    @classmethod
    def add_time(cls, grid_coord: bool = True):
        """Creates a new class with a time variable added. Equivalent to using:

        from geo_skeletons.decorators import add_time

        @add_time()
        class NewClass(OldClass):
            pass"""
        new_cls = type(_modified_name(cls.__name__), (cls,), {})
        return add_time(grid_coord=grid_coord)(new_cls)

    @classmethod
    def add_frequency(
        cls, name: Union[str, MetaParameter] = gp.wave.Freq, grid_coord: bool = False
    ):
        """Creates a new class with a frequency variable added. Equivalent to using:

        from geo_skeletons.decorators import add_frequency

        @add_frequency()
        class NewClass(OldClass):
            pass"""
        new_cls = type(_modified_name(cls.__name__), (cls,), {})
        return add_frequency(name=name, grid_coord=grid_coord)(new_cls)

    @classmethod
    def add_direction(
        cls,
        name: Union[str, MetaParameter] = gp.wave.Dirs,
        grid_coord: bool = False,
        dir_type: Optional[bool] = None,
    ):
        """Creates a new class with a direction variable added. Equivalent to using:

        from geo_skeletons.decorators import add_direction

        @add_direction()
        class NewClass(OldClass):
            pass"""
        new_cls = type(_modified_name(cls.__name__), (cls,), {})
        return add_direction(name=name, grid_coord=grid_coord, dir_type=dir_type)(
            new_cls
        )

    @classmethod
    def add_coord(
        cls,
        name: Union[str, MetaParameter] = "dummy",
        grid_coord: bool = False,
    ):
        """Creates a new class with a coordinate added. Equivalent to using:

        from geo_skeletons.decorators import add_coord

        @add_coord('new_coord')
        class NewClass(OldClass):
            pass"""
        new_cls = type(_modified_name(cls.__name__), (cls,), {})
        return add_coord(name=name, grid_coord=grid_coord)(new_cls)

    @classmethod
    def add_datavar(
        cls,
        name: Union[str, MetaParameter],
        coord_group: str = "all",
        default_value: float = 0.0,
    ) -> None:
        """Creates a new class with a data variable added.

        name: name of variable
        coord_group: 'all', 'spatial', 'grid' or 'gridpoint'
        default_value: float
        dir_type (for directional parameters): 'from', 'to' or 'math' (Autimatically parsed if name is a MetaParameter)

        Equivalent to using:

        from geo_skeletons.decorators import add_coord

        @add_datavar('new_var')
        class NewClass(OldClass):
            pass"""
        new_cls = type(_modified_name(cls.__name__), (cls,), {})
        return add_datavar(
            name=name, coord_group=coord_group, default_value=default_value
        )(new_cls)

    @classmethod
    def add_magnitude(
        cls,
        name: Union[str, MetaParameter],
        x: str,
        y: str,
        direction: Optional[Union[str, MetaParameter]] = None,
        dir_type: Optional[str] = None,
    ) -> None:
        """Adds a magnitude to an instance of a (non-static) Skeleton.

        Similar to using @add_magnitude on a class, but only affects an instance.

        name: name of variable
        x [str]: name of already set variable that will be used as x-component
        y [str]: name of already set variable that will be used as y-component
        direction: name of the direction of the magnitude being set
        dir_type: 'from', 'to' or 'math'"""
        new_cls = type(_modified_name(cls.__name__), (cls,), {})
        return add_magnitude(
            name=name, x=x, y=y, direction=direction, dir_type=dir_type
        )(new_cls)

    @classmethod
    def add_mask(
        cls,
        name: Union[str, MetaParameter],
        default_value: int = 0,
        coord_group: str = "grid",
        opposite_name: Optional[Union[str, MetaParameter]] = None,
        triggered_by: Optional[str] = None,
        valid_range: tuple[float] = (0.0, None),
        range_inclusive: bool = True,
    ):
        """Adds a mask t an instans of a (non-static) Skeleton.

        Similar to using @add_mask on a class, but only affects an instance.

        default_value = 0 or 1 (False or True)
        coord_group = 'all', 'spatial', 'grid' or 'gridpoint'
        opposite_name: E.g. 'land' is name = 'sea'
        triggered_by: E.g. land_mask might be triggered by setting variable 'hs'
        valid_range: Range that mask is set by the triggered variable. None is infiniate.
        range_inclusive (default_true): E.g. valid_range (0.0, None) includes all non-negative values.
        """

        new_cls = type(_modified_name(cls.__name__), (cls,), {})
        return add_mask(
            name=name,
            default_value=default_value,
            coord_group=coord_group,
            opposite_name=opposite_name,
            triggered_by=triggered_by,
            valid_range=valid_range,
            range_inclusive=range_inclusive,
        )(new_cls)

    @classmethod
    def from_coord_dict(cls, coord_dict):
        """Creates an empty version of the class from a dictionary containing the coordinates"""
        return cls(**coord_dict)

    @classmethod
    def from_netcdf(cls, filename: str, **kwargs) -> "Skeleton":
        """Generates a instance of the Skeleton class from a netcdf.

        For information about the keywords, see the from_ds-method"""
        return cls.from_ds(
            xr.open_dataset(filename), name=f"Created from {filename}", **kwargs
        )

    @classmethod
    def from_ds(
        cls,
        ds: xr.Dataset,
        chunks: Optional[Union[tuple[int], str]] = None,
        only_vars: Optional[list[str]] = None,
        ignore_vars: Optional[list[str]] = None,
        keep_ds_names: bool = False,
        decode_cf: bool = True,
        core_aliases: dict[Union[MetaParameter, str], str] = None,
        ds_aliases: dict[str, Union[MetaParameter, str]] = None,
        dynamic: bool = False,
        verbose: bool = False,
        meta_dict: dict = None,
        name: str = "LonelySkeleton",
        **kwargs,
    ) -> "Skeleton":
        """Generats an instance of a Skeleton from an xarray Dataset.

        only_vars [default [], i.e. read all]: list of ds-variable names that will be read
        ignore_vars [default []]: list of ds-variables to ignore. [Default None]

        keep_ds_names [default False]: Uses the Dataset variable names instead of the geo-parameter default short names
        decode_cf [default True]: Allow decoding of cf standard names

        core_aliases [default {}]: dict mapping the existing variables in the Skeleton to a variable name in the Dataset
        ds_aliases [default {}]: dict describing the ds-variables in terms of geo-parameters or strings

        dynamic [default: False] Allows creation of new data variables. Otherwise limited to existing variables.

        Core aliases
        ------------------------------------------------------------------------
        Ex1: core_aliases = {'hs': 'Hm0'}
            Reads the Dataset variable 'Hm0' and sets the Skeleton variable 'hs'
        Ex2: core_aliases = {gp.wave.Hs: 'Hm0'}
            Reads the Dataset variable 'Hm0' and set the Skeleton variable that matches the gp.wave.Hs geo-parameter (if unambiguous)


        Dataset aliases
        ------------------------------------------------------------------------
        Ex1: ds_aliases = {'Hm0', 'hs'}
            Reads the Dataset variable 'Hm0', creates and sets a Skeleton variable 'hs'
        Ex2: core_aliases = {'Hm0': gp.wave.Hs}
            Reads the Dataset variable 'Hm0', creates and sets a Skeleton variable with the default name 'hs' using metadata from gp.wave.Hs
        Ex3: core_aliases = {'Hm0': gp.wave.Hs('hsig')}
            Reads the Dataset variable 'Hm0', creates and sets a Skeleton variable 'hsig' using metadata from gp.wave.Hs
        Ex4: core_aliases = {'Hm0': gp.wave.Hs}, keep_ds_names = True
            Reads the Dataset variable 'Hm0', creates and sets a Skeleton variable with the name 'Hm0' using metadata from gp.wave.Hs


        NB! Method uses internal relationships defined inside the geo-parameters module (see e.g. gp.wind.WindDir.my_family())
        ------------------------------------------------------------------------
        Ex1: gp.wave.Tp defined in class -> can be read using gp.wave.Fp in the Dataset (known inverse)
        Ex2: gp.wind.WindDir defined in class -> can be read using gp.wind.WindDirTo in the Dataset (known opposite direction)
        Ex3: gp.wind.WindDir defined in class -> can be read using gp.wind.XWind and gp.wind.YWind in the Dataset (known components)


        NB! Method uses an internal set of known aliases for some parameters
        ------------------------------------------------------------------------
        It is NOT RECOMMENDED to RELY on these mappings, since they are in no way exhaustive.
        If no metadata is present, it is better to use the ds_aliases keyword to assign a geo-parameter for each variable.

        Ex1: 'lon' <-> 'longitude' <-> gp.grid.Lon
        Ex2: 'hs' <-> 'swh' <-> 'hm0' <-> 'hsig' <-> 'h13' <-> 'vhm0' <-> gp.wave.Hs
        Ex3: 'xwnd' <-> 'xwnd' <-> gp.wind.XWind
        Ex4: 'Pdir' not mapped to anything, since directional variables have to/from ambiguity without any metadata
        Ex5: 'Tm' not mapped to anything, since it can be used interchangeably for gp.wave.Tm01 and gp.wave.Tm_10




        **kwargs: Can be used to provide missing coordinates
        ------------------------------------------------------------------------
        Ex: The z-variable doesn't exist in the DataSet:
            new_instance = SkeletonClass.from_ds(ds, z=[1,2,3])
        """

        meta_dict = meta_dict or {}
        core_aliases = core_aliases or {}
        ds_aliases = ds_aliases or {}
        only_vars = only_vars or []
        ignore_vars = ignore_vars or []

        if dynamic:  # Try to decode variables from the dataset
            cls = create_new_class_dynamically(
                cls=cls,
                ds=ds,
                only_vars=only_vars,
                ignore_vars=ignore_vars,
                keep_ds_names=keep_ds_names,
                decode_cf=decode_cf,
                core_aliases=core_aliases,
                ds_aliases=ds_aliases,
                extra_coords=kwargs,
                verbose=verbose,
            )

        # These are the mappings identified in the ds. Might miss some that are provided as keywords
        (
            core_coords_to_ds_coords,
            core_vars_to_ds_vars,
            coords_needed,
        ) = identify_core_in_ds(
            cls.core,
            ds,
            aliases=core_aliases,
            ds_aliases=ds_aliases,
            ignore_vars=ignore_vars,
            only_vars=only_vars,
            allowed_misses=list(kwargs.keys()),
            verbose=verbose,
        )

        coords = gather_coord_values(
            coords_needed, ds, core_coords_to_ds_coords, extra_coords=kwargs
        )

        name = ds.attrs.get("name") or name
        points = cls(**coords, chunks=chunks, name=name)
        # Lengths needed for matching coordinates with wrong name
        # We do this instead of reading the lengths of the arrays directyl
        # Reason is that we want 'inds' for PointSkeletons etc
        core_lens = {c: len(points.get(c)) for c in points.core.coords("all")}

        # Remap the names of the Dataset dimension so that we can traspose data when setting
        ds_remapped_coords, __ = remap_coords_of_ds_vars_to_skeleton_names(
            ds, cls.core, core_vars_to_ds_vars, core_coords_to_ds_coords, core_lens
        )

        # Set data
        points = set_core_vars_to_skeleton_from_ds(
            points,
            ds,
            core_vars_to_ds_vars,
            ds_remapped_coords,
            meta_dict,
        )

        metadata = meta_dict.get("_global_") or ds.attrs
        points.meta.set(metadata)

        return points

    def absorb(self, skeleton_to_absorb: "Skeleton", dim: str) -> "Skeleton":
        """Absorb another object of same type over a centrain dimension.
        For a PointSkeleton the inds-variable reorganized if dim='inds' is given."""
        if not self.is_gridded() and dim == "inds":
            inds = skeleton_to_absorb.inds() + len(self.inds())
            skeleton_to_absorb.ds()["inds"] = inds

        new_skeleton = self.from_ds(
            xr.concat(
                [self.ds(), skeleton_to_absorb.ds()], dim=dim, data_vars="minimal"
            ).sortby(dim)
        )
        return new_skeleton

    def cut_to_common_times(self, skeleton_to_compare_with: "Skeleton") -> "Skeleton":
        """Cuts the skeletons to cover only the coinciding times in the two skeletons.

        Returns a tuple of skeletons that have identical times."""
        if not "time" in skeleton_to_compare_with.core.coords():
            raise SkeletonError("Provided Skeleton does not have a time dimension!")
        if not "time" in self.core.coords():
            raise SkeletonError("Skeleton does not have a time dimension!")
        common_times = (
            self.time(data_array=True) - skeleton_to_compare_with.time(data_array=True)
        ).time.values

        return self.sel(time=common_times), skeleton_to_compare_with.sel(
            time=common_times
        )

    def sel(self, **kwargs) -> "Skeleton":
        """Creates a new instance by selecting only some of the wanted variables.
        e.g. new_skeleton = skeleton.sel(lon=slice(10,20))

        Calls the Xarray .sel method on the underlying DataSet"""

        # Xarray cant slice longitude and latitude if defined over inds
        lon_slice = kwargs.get("lon")
        lat_slice = kwargs.get("lat")
        x_slice = kwargs.get("x")
        y_slice = kwargs.get("y")
        if not self.is_gridded():
            slice_inds = self._determine_slice_inds(lon_slice, lat_slice, "lon", "lat")
            if slice_inds is None:
                slice_inds = self._determine_slice_inds(x_slice, y_slice, "x", "y")

            if lon_slice is not None:
                del kwargs["lon"]
            if lat_slice is not None:
                del kwargs["lat"]
            if x_slice is not None:
                del kwargs["x"]
            if y_slice is not None:
                del kwargs["y"]

            if slice_inds is not None:
                return self.sel(inds=slice_inds, **kwargs)

        return self.from_ds(
            self.ds().sel(**kwargs),
            data_vars=self.core.non_coord_objects(),
            keep_ds_names=True,
            name=self.name,
        )

    def _determine_slice_inds(self, x_slice, y_slice, x: str, y: str):
        """Determines the indeces of e.g. a lon-slice for a PointSkeleton"""
        if x_slice is None and y_slice is None:
            return None

        x_not_a_slice = x_slice is not None and not isinstance(x_slice, slice)
        y_not_a_slice = y_slice is not None and not isinstance(y_slice, slice)

        if x_not_a_slice and y_not_a_slice:
            inds_dict = self.yank_point(**{x: x_slice, y: y_slice})
            return inds_dict["inds"]
        else:
            x_inds = _determine_inds(x_slice, self.get(x))
            y_inds = _determine_inds(y_slice, self.get(y))

            return np.array(list(set(x_inds).intersection(set(y_inds))))

    def isel(self, **kwargs) -> "Skeleton":
        """Creates a new instance by selecting only some of the wanted variables.
        e.g. new_skeleton = skeleton.isel(lon=[0,1,2])

        Calls the Xarray .isel method on the underlying DataSet"""
        return self.from_ds(
            self.ds().isel(**kwargs),
            data_vars=self.core.non_coord_objects(),
            keep_ds_names=True,
            name=self.name,
        )

    def insert(self, name: str, data: np.ndarray, **kwargs) -> None:
        """Inserts a slice of data into the Skeleton.

        If data named 'geodata' has shape dimension ('time', 'inds', 'threshold') and shape (57, 10, 3), then
        data_slice having the threshold=0.4 and time='2023-11-08 12:00:00' having shape=(10,) can be inserted by using the values:

        skeleton.insert(name='geodata', data=data_slice, time='2023-11-08 12:00:00', threshold=0.4)
        """
        coord_group = self.core.coord_group(name)
        dims = self.core.coords(coord_group)

        index_kwargs = {}
        for dim in dims:
            val = kwargs.get(dim)
            if val is not None:
                index_kwargs[dim] = np.where(self.get(dim) == val)[0][0]

        self.ind_insert(name=name, data=data, **index_kwargs)

    def ind_insert(self, name: str, data: np.ndarray, **kwargs) -> None:
        """Inserts a slice of data into the Skeleton.

        If data named 'geodata' has dimension ('time', 'inds', 'threshold') and shape (57, 10, 3), then
        data_slice having the first threshold and first time can be inserted by using the index values:

        skeleton.ind_insert(name='geodata', data=data_slice, time=0, threshold=0)"""

        coord_group = self.core.coord_group(name)
        dims = self.core.coords(coord_group)
        index_list = list(np.arange(len(dims)))

        for n, dim in enumerate(dims):
            var = self.get(dim)
            if var is None:
                raise KeyError(f"No coordinate {dim} exists!")
            ind = kwargs.get(dim, slice(len(var)))
            index_list[n] = ind

        old_data = self.get(name, squeeze=False).copy()
        N = len(old_data.shape)
        data_str = "old_data["
        for n in range(N):
            data_str += f"{index_list[n]},"
        data_str = data_str[:-1]
        data_str += "] = data"
        exec(data_str)
        self.set(name, old_data, allow_reshape=False)
        return

    def set(
        self,
        name: Union[str, MetaParameter],
        data: Optional[Union[np.ndarray, xr.DataArray]] = None,
        dir_type: Optional[str] = None,
        allow_reshape: bool = True,
        allow_transpose: bool = False,
        coords: Optional[list[str]] = None,
        silent: bool = True,
        chunks: Optional[Union[tuple, str]] = None,
    ) -> None:
        """Sets the data using the following logic:

        data [None]: numpy/dask array. If None [Default], an empty array is set.

        Any numpy array is converted to a dask-array if dask-mode is set with .dask.activate().
        If keyword 'chunks' is set, then conversion to dask is always done.

        If given data is a dask array, then it is never rechunked, but used as is.

        allow_reshape [True]: Allow squeezing out trivial dimensions.
        allow_transpose [False]: Allow trying to transpose exactly two non-trivial dimensions

        Otherwise, data is assumed to be in the right dimension, but can also be reshaped:

        1) If 'coords' (e.g. ['freq',' inds']) is given, then data is reshaped assuming data is in that order.
        2) If data is a DataArray, then 'coords' is set using the information in the DataArray.
        3) If data has any trivial dimensions, then those are squeezed.
        4) If data is missing any trivial dimension, then those are expanded.
        5) If data along non-trivial dimensions is two-dimensional, then a transpose is attemted.

        NB! For 1), only non-trivial dimensions need to be identified

        silent [True]: Don't output what reshaping is being performed.

        """

        if not isinstance(name, str) and not gp.is_gp(name):
            raise TypeError(
                f"'name' must be of type 'str', or 'MetaParameter' not '{type(name).__name__}'!"
            )

        if gp.is_gp(name):
            names = self.core.find(name)
            if len(names) == 0:
                raise UnknownVariableError(f"Variable matching {name} not found!")
            if len(names) > 1:
                raise UnknownVariableError(
                    f"Found several variables ({names}) matching {name}!"
                )
            name = names[0]

        first_set = (
            name in self._ds_manager.empty_vars()
            or name in self._ds_manager.empty_masks()
        )

        if data is None:
            data = self.get(name, empty=True, squeeze=False, dir_type=dir_type)

        data = dask_computations.atleast_1d(data)

        # Make constant array if given data has no shape
        # Return original data if it has shape
        data = self.dask.constant_array(data, self.shape(name), chunks=chunks)

        # If a DataArray is given, then read the dimensions from there if not explicitly provided in a keyword
        if isinstance(data, xr.DataArray):
            coords = coords or list(data.dims)
            data = data.data

        if not self.dask.is_active() and chunks is None:
            data = self.dask.undask_me(data)
        else:
            data = self.dask.dask_me(data, chunks=chunks)

        data = self._reshape_data(
            name,
            data,
            coords,
            silent,
            allow_reshape,
            allow_transpose,
        )

        if dir_type not in ["to", "from", "math", None]:
            raise ValueError(
                f"'dir_type' needs to be 'to', 'from' or 'math' (or None), not {dir_type}"
            )

        # Masks are stored as integers
        if name in self.core.masks("all"):
            data = data.astype(int)

        if name in self.core.magnitudes("all"):
            if dir_type:
                raise DirTypeError
            self._set_magnitude(
                name=name,
                data=data,
            )
        elif name in self.core.directions("all"):
            self._set_direction(
                name=name,
                data=data,
                dir_type=dir_type,
            )
        else:
            self._set_data(
                name=name,
                data=data,
                dir_type=dir_type,
            )

        if first_set:
            self.meta.metadata_to_ds(name)

        return

    def _reshape_data(
        self,
        name: str,
        data: np.ndarray,
        coords: list[str],
        silent: bool,
        allow_reshape: bool,
        allow_transpose: bool,
    ) -> np.ndarray:
        """Reshapes the data using the following logic:

        allow_reshape [True]: Allow squeezing out trivial dimensions.
        allow_transpose [False]: Allow trying to transpose exactly two non-trivial dimensions

        Otherwise, data is assumed to be in the right dimension, but can also be reshaped:

        1) If 'coords' (e.g. ['freq',' inds']) is given, then data is reshaped assuming data is in that order.
        2) If data is a DataArray, then 'coords' is set using the information in the DataArray.
        3) If data has any trivial dimensions, then those are squeezed.
        4) If data is missing any trivial dimension, then those are expanded.
        5) If data along non-trivial dimensions is two-dimensional, then a transpose is attemted.

        NB! For 1), only non-trivial dimensions need to be identified

        silent [True]: Don't output what reshaping is being performed.

        If data cannot be reshaped, a DataWrongDimensionError is raised.
        """
        reshape_manager = ReshapeManager(silent=silent)
        coord_type = self.core.coord_group(name)

        # Do explicit reshape if coordinates of the provided data is given
        if coords is not None:
            # Some dimensions of the provided data might not exist in the Skeleton
            # If they are trivial then they don't matter, so squeeze them out
            squeezed_coords = [
                c for c in coords if self.get(c) is not None and len(self.get(c)) > 1
            ]

            data = reshape_manager.explicit_reshape(
                data.squeeze(),
                data_coords=squeezed_coords,
                expected_coords=self.core.coords(coord_type),
            )

            # Unsqueeze the data before setting to get back trivial dimensions
            data = reshape_manager.unsqueeze(data, expected_shape=self.size(coord_type))

        # Try to set the data
        if data.shape != self.shape(name):
            # If we are here then the data could not be set, but we are allowed to try to reshape
            if not silent:
                print(f"Size of {name} does not match size of {type(self).__name__}...")

            # Save this for messages
            original_data_shape = data.shape

            if allow_transpose:
                data = reshape_manager.transpose_2d(
                    data, expected_squeezed_shape=self.size(coord_type, squeeze=True)
                )
            if allow_reshape:
                data = reshape_manager.unsqueeze(
                    data, expected_shape=self.size(coord_type)
                )
            if data is None or (data.shape != self.shape(name)):
                raise DataWrongDimensionError(
                    original_data_shape, self.shape(name)
                )  # Reshapes have failed or were not tried

            if not silent:
                print(f"Reshaping data {original_data_shape} -> {data.shape}...")
        return data

    def _set_magnitude(
        self,
        name: str,
        data: np.ndarray,
    ) -> None:
        """Sets a magnitude variable.

        Calculates x- and y- components and sets them based on set connected direction.

        Data needs to be exactly right shape."""
        obj = self.core.get(name)
        x_component, y_component = obj.x, obj.y
        dir_data = self.get(obj.direction.name, dir_type="math", squeeze=False)

        s = dask_computations.sin(dir_data)
        c = dask_computations.cos(dir_data)
        ux = data * c
        uy = data * s
        self._set_data(
            name=x_component,
            data=ux,
        )
        self._set_data(
            name=y_component,
            data=uy,
        )

    def _set_direction(
        self,
        name: str,
        data: np.ndarray,
        dir_type: str,
    ) -> None:
        """Sets a directeion variable.

        Calculates x- and y- components and sets them based on set connected magnitude.

        Data needs to be exactly right shape."""
        obj = self.core.get(name)
        x_component, y_component = obj.x, obj.y
        mag_data = self.get(obj.magnitude.name, squeeze=False)

        dir_type = dir_type or obj.dir_type

        data = dir_conversions.convert_to_math_dir(data, dir_type)

        s = dask_computations.sin(data)
        c = dask_computations.cos(data)
        ux = mag_data * c
        uy = mag_data * s

        self._set_data(
            name=x_component,
            data=ux,
        )
        self._set_data(
            name=y_component,
            data=uy,
        )

    def _set_data(
        self,
        name: str,
        data: np.ndarray,
        dir_type: str = None,
    ) -> None:
        """Sets a data variable to the underlying dataset.

        Data needs to be exactly right shape.

        Triggers setting metadata of the variable and possible connected masks."""
        if dir_type not in ["to", "from", "math", None]:
            raise ValueError(
                f"'dir_type' needs to be 'to', 'from' or 'math' (or None), not {dir_type}"
            )
        set_dir_type = self.core.get_dir_type(name)
        if dir_type is not None and set_dir_type is None:
            raise DirTypeError

        dir_type = dir_type or set_dir_type
        data = dir_conversions.convert(data, in_type=dir_type, out_type=set_dir_type)
        self._ds_manager.set(data=data, name=name)
        self.meta.metadata_to_ds(name)
        self._trigger_masks(name, data)

    def _trigger_masks(self, name: str, data: Union[np.ndarray, xr.DataArray]) -> None:
        """Set any masks that are triggered by setting a specific data variable
        E.g. Set new 'land_mask' when 'topo' is set."""
        for mask in self.core.triggers(name):

            if mask.range_inclusive[0]:
                low_mask = data >= mask.valid_range[0]
            else:
                low_mask = data > mask.valid_range[0]

            if mask.range_inclusive[1]:
                high_mask = data <= mask.valid_range[1]
            else:
                high_mask = data < mask.valid_range[1]

            mask_array = np.logical_and(low_mask, high_mask)
            self.set(mask.name, mask_array)

    def get(
        self,
        name: str,
        strict: bool = False,
        empty: bool = False,
        data_array: bool = False,
        dir_type: Optional[str] = None,
        squeeze: bool = True,
        dask: Optional[bool] = None,
        **kwargs,
    ) -> Union[np.ndarray, xr.DataArray]:
        """Gets a mask or data variable as an array.

        strict [False]: Return 'None' if data not set. Otherwise returns empty array.
        empty [False]: Return an array full with default values even if variable is set.
        data_array [False]: Return data as an xarray DataArray.
        squeeze [True]: Smart squeeze out trivial dimensions, but return at least 1d array.
        boolean_mask [False]: Convert array to a boolean array.
        dask [None]: Return dask array [True] or numpy array [False]. Default: Use set dask-mode
        """
        if self.ds() is None:
            return None

        if not isinstance(name, str) and not gp.is_gp(name):
            raise TypeError(
                f"'name' must be of type 'str', or 'MetaParameter' not '{type(name).__name__}'!"
            )

        if gp.is_gp(name):
            names = self.core.find(name)
            if len(names) == 0:
                raise UnknownVariableError(f"Variable matching {name} not found!")
            if len(names) > 1:
                raise UnknownVariableError(
                    f"Found several variables ({names}) matching {name}!"
                )
            name = names[0]

        if name == "x":
            return self.x(strict=strict, **kwargs)
        elif name == "y":
            return self.y(strict=strict, **kwargs)
        elif name == "lon":
            return self.lon(strict=strict, **kwargs)
        elif name == "lat":
            return self.lat(strict=strict, **kwargs)

        if dir_type not in ["to", "from", "math", None]:
            raise ValueError(
                f"'dir_type' needs to be 'to', 'from' or 'math' (or None), not {dir_type}"
            )

        if name in self.core.magnitudes():
            if dir_type:
                raise DirTypeError
            data = self._get_magnitude(
                name=name,
                strict=strict,
                empty=empty,
                **kwargs,
            )
        elif name in self.core.directions():
            data = self._get_direction(
                name=name,
                strict=strict,
                dir_type=dir_type,
                empty=empty,
                **kwargs,
            )
        elif name in self.core.mask_points():
            lon, lat = eval(f"self.{name}(strict=strict, **kwargs)")
            return lon, lat

        elif name in self.core.masks():
            mask_is_secondary = not self.core._mask_is_primary(name)
            if mask_is_secondary:
                primary_name = self.core._find_primary_mask(name)
            else:
                primary_name = name
            data = self._get_data(
                name=primary_name,
                strict=strict,
                dir_type=dir_type,
                empty=empty,
                **kwargs,
            )
            if mask_is_secondary:
                data = np.logical_not(data).astype(int)
        else:
            data = self._get_data(
                name=name,
                strict=strict,
                dir_type=dir_type,
                empty=empty,
                **kwargs,
            )

        if not isinstance(data, xr.DataArray):
            return None

        # The coordinates are never given as dask arrays
        if name in self.core.coords("all"):
            dask = False

        if name in self.core.masks("all"):
            data = data.astype(bool)

        if squeeze:
            data = self._smart_squeeze(name, data)
        # Use dask mode default if not explicitly overridden

        if dask is None:  # Use DaskManger defaults
            if self.dask.is_active():
                data = self.dask.dask_me(data)
            elif self.dask.data_is_dask(
                data
            ):  # Don't comput array since not explicitly requested
                pass
            else:
                data = self.dask.undask_me(data)
        elif dask:  # Force dask array even if dask-mode deactivated
            data = self.dask.dask_me(data, force=True)
        elif not dask:
            data = self.dask.undask_me(data)

        if not data_array:
            data = data.data
            if name == "time":
                data = pd.to_datetime(data)

        return data

    def _get_direction(
        self,
        name: str,
        strict: bool,
        empty: bool,
        dir_type: str,
        **kwargs,
    ) -> xr.DataArray:

        x_data = self._ds_manager.get(
            self.core.get(name).x,
            empty=empty,
            strict=strict,
            **kwargs,
        )
        y_data = self._ds_manager.get(
            self.core.get(name).y,
            empty=empty,
            strict=strict,
            **kwargs,
        )

        if x_data is None or y_data is None:
            return None

        if not self.dask.is_active() and (
            empty or self._ds_manager.get(self.core.get(name).y, strict=True) is None
        ):
            y_data = self.dask.undask_me(y_data)
        if not self.dask.is_active() and (
            empty or self._ds_manager.get(self.core.get(name).x, strict=True) is None
        ):
            x_data = self.dask.undask_me(x_data)

        dir_type = dir_type or self.core.get(name).dir_type
        data = dir_conversions.compute_math_direction(x_data, y_data)
        data = dir_conversions.convert_from_math_dir(data, dir_type=dir_type)
        data = data.assign_attrs(self.meta.get(name))

        return data

    def _get_magnitude(
        self,
        name: str,
        strict: bool,
        empty: bool,
        **kwargs,
    ) -> xr.DataArray:
        x_data = self._ds_manager.get(
            self.core.get(name).x,
            empty=empty,
            strict=strict,
            **kwargs,
        )
        y_data = self._ds_manager.get(
            self.core.get(name).y,
            empty=empty,
            strict=strict,
            **kwargs,
        )

        if x_data is None or y_data is None:
            return None

        if not self.dask.is_active() and (
            empty or self._ds_manager.get(self.core.get(name).y, strict=True) is None
        ):
            y_data = self.dask.undask_me(y_data)
        if not self.dask.is_active() and (
            empty or self._ds_manager.get(self.core.get(name).x, strict=True) is None
        ):
            x_data = self.dask.undask_me(x_data)

        data = dir_conversions.compute_magnitude(x_data, y_data)
        data = data.assign_attrs(self.meta.get(name))

        return data

    def _get_data(
        self,
        name: str,
        strict: bool,
        empty: bool,
        dir_type: str,
        **kwargs,
    ) -> xr.DataArray:
        data = self._ds_manager.get(name, empty=empty, strict=strict, **kwargs)

        if not self.dask.is_active() and (
            empty or self._ds_manager.get(name, strict=True) is None
        ):
            data = self.dask.undask_me(data)

        if data is None:
            return None

        set_dir_type = self.core.get_dir_type(name)
        if dir_type is not None and set_dir_type is None:
            raise DirTypeError
        dir_type = dir_type or set_dir_type
        data = dir_conversions.convert(data, in_type=set_dir_type, out_type=dir_type)
        return data

    def _smart_squeeze(self, name: str, data: xr.DataArray) -> xr.DataArray:
        """Squeezes the data but takes care that one dimension is kept.

        Spatial dims are not protected, but if the results is a 0-dim,
        then 'inds' or 'lat' is kept."""
        dims_to_drop = [
            dim
            for dim in self.core.coords("all")
            if self.shape(dim)[0] == 1 and dim in data.dims
        ]

        # If it looks like we are dropping all coords, then save the spatial ones
        if dims_to_drop == list(data.coords):
            dims_to_drop = list(
                set(dims_to_drop) - set(self.core.coords("spatial")) - set([name])
            )

        if dims_to_drop:
            data = data.squeeze(dim=dims_to_drop, drop=True)

        # If slicing down to one point, we have to make sure we still have some dimension in the DataArray
        if data.shape == ():
            if "inds" in self.core.coords("spatial"):
                coord_to_create = "inds"
            else:
                coord_to_create = "lat"
            val = np.atleast_1d(data[coord_to_create].values)

            data = self._ds_manager.force_compile_data_array(
                np.atleast_1d(data.values), {coord_to_create: val}
            )

        return data

    def coord_squeeze(self, coords: list[str]) -> list[str]:
        """Smart squeezes a list of coordinates according to the following rules:

        1) Empty list return empty
        2) Trivial one coordinate list returns itself
        3) All coordinates that are of trivial length are removed and returned if not empty
        4) If 3) resulted in an empty list, then ['inds'], ['lat']/['y'] or ['lon']/['x'] is returned
        """
        if not coords or len(coords) == 1:
            return coords

        long_coords = [c for c in coords if len(self.get(c)) > 1]

        if long_coords:
            return long_coords

        present_spatial_coords = set(coords).intersection(self.core.coords("spatial"))
        if not present_spatial_coords:
            return []

        if "inds" in present_spatial_coords:
            return ["inds"]
        if "lat" in present_spatial_coords:
            return ["lat"]
        if "y" in present_spatial_coords:
            return ["y"]
        if "lon" in present_spatial_coords:
            return ["lon"]
        if "x" in present_spatial_coords:
            return ["x"]

    def ds(self, compile: bool = False) -> Union[xr.Dataset, None]:
        """Returns the underlying Xarray Dataset. None if dosen't exist.

        compile [default False]: Add magnitudes and directions to the Dataset (performs deepcopy!)
        """
        if not hasattr(self, "_ds_manager"):
            return None
        ds = self._ds_manager.ds()
        if compile:
            ds = deepcopy(ds)
            for mag in self.core.magnitudes():
                ds[mag] = self.get(mag, data_array=True)
            for dirs in self.core.directions():
                ds[dirs] = self.get(dirs, data_array=True)
        return ds

    def size(
        self, coord_group: str = "all", squeeze: bool = False, **kwargs
    ) -> tuple[int]:
        """Returns the size of the Skeleton.

        'all' [default]: size of entire Skeleton
        'spatial': size over coordinates from the Skeleton (x, y, lon, lat, inds)
        'grid': size over coordinates for the grid (e.g. z, time) and the spatial coordinates
        'gridpoint': size over coordinates for a grid point (e.g. frequency, direcion or time)
        """
        if coord_group not in ["all", "spatial", "grid", "gridpoint"]:
            raise KeyError(
                f"coords should be 'all', 'spatial', 'grid' or 'gridpoint', not {coord_group}!"
            )
        coords = self.core.coords(coord_group)
        if squeeze:
            coords = self.coord_squeeze(coords)

        return self._ds_manager.coords_to_size(coords, **kwargs)

    def shape(self, var, squeeze: bool = False, **kwargs) -> tuple[int]:
        """Returns the size of one specific data variable."""
        if var in self.core.coords("all"):
            return self.get(var, squeeze=False).shape
        coord_group = self.core.coord_group(var)
        return self.size(coord_group=coord_group, squeeze=squeeze, **kwargs)

    def inds(self, **kwargs) -> np.ndarray:
        """Returns the index variable for PointSkeletons. Defaults to None for GriddedSkeletons."""
        return self.get("inds", **kwargs)

    def edges(
        self,
        coord: str,
        native: bool = False,
        strict: bool = False,
        utm: tuple[int, str] = None,
    ) -> tuple[float, float]:
        """Min and max values of x. Conversion made for sperical grids."""
        if coord not in ["x", "y", "lon", "lat"]:
            print("coord need to be 'x', 'y', 'lon' or 'lat'.")
            return

        if coord in ["x", "y"]:
            x, y = self.xy(native=native, strict=strict, utm=utm)
        else:
            x, y = self.lonlat(native=native, strict=strict, utm=utm)

        if coord in ["x", "lon"]:
            val = x
        else:
            val = y

        if val is None:
            return (None, None)

        return float(np.min(val)), float(np.max(val))

    def extent(self, coord: str, strict: bool = False) -> float:
        """Gives the extent in metres in x- or y-direction.

        For spherical grids this might differ from difference of
        edges in case of gridded data or high latitudes that
        cause nan in edges."""
        if coord not in ["x", "y"]:
            print("coord need to be 'x' or 'y'.")
            return

        if not self.core.is_cartesian() and strict:
            return None

        if self.core.is_cartesian():
            return np.diff(self.edges(coord))[0]

        if coord == "x":
            lon1, lon2 = self.edges("lon")
            lat = np.median(self.lat())
            return distance_2points(lat1=lat, lon1=lon1, lat2=lat, lon2=lon2)
        else:
            lat1, lat2 = self.edges("lat")
            lon = np.median(self.lon())
            return distance_2points(lat1=lat1, lon1=lon, lat2=lat2, lon2=lon)

    def nx(self) -> int:
        """Length of x/lon-vector."""
        return len(self.x(native=True, suppress_warning=True))

    def ny(self) -> int:
        """Length of y/lat-vector."""
        return len(self.y(native=True, suppress_warning=True))

    def dx(self, native: bool = False, strict: bool = False) -> float:
        """Mean grid spacing of the x vector. Conversion made for spherical grids."""
        if not self.core.is_cartesian() and strict and (not native):
            return None

        if self.nx() == 1:
            return 0.0

        return float(
            max(self.x(native=native, suppress_warning=True))
            - min(self.x(native=native, suppress_warning=True))
        ) / (self.nx() - 1)

    def dy(self, native: bool = False, strict: bool = False) -> float:
        """Mean grid spacing of the y vector. Conversion made for spherical grids."""
        if not self.core.is_cartesian() and strict and (not native):
            return None

        if self.ny() == 1:
            return 0.0

        return float(
            max(self.y(native=native, suppress_warning=True))
            - min(self.y(native=native, suppress_warning=True))
        ) / (self.ny() - 1)

    def dlon(self, native: bool = False, strict: bool = False):
        """Mean grid spacing of the longitude vector. Conversion made for cartesian grids."""
        if self.nx() == 1:
            return 0.0

        lon = self.lon(native=native, strict=strict, suppress_warning=True)
        if lon is None:
            return None

        return float(max(lon) - min(lon)) / (self.nx() - 1)

    def dlat(self, native: bool = False, strict: bool = False):
        """Mean grid spacing of the latitude vector. Conversion made for
        cartesian grids."""
        if self.ny() == 1:
            return 0.0
        lat = self.lat(native=native, strict=strict, suppress_warning=True)
        if lat is None:
            return None

        return float(max(lat) - min(lat)) / (self.ny() - 1)

    def coord_dict(self, coord_group: str = "all") -> dict[str, np.ndarray]:
        """Returns a coord dictionary containing all coordinates of the given coordinate group.

        for 'all' the dict can be used to recreate the Skeleton."""
        coords = list(set(self.core.coords(coord_group)) - set(["inds"]))
        if coord_group in ["all", "spatial", "grid"]:
            coords = coords + self.core.data_vars("spatial")
        return {c: self.get(c) for c in coords}

    def yank_point(
        self,
        lon: Union[float, Iterable[float]] = None,
        lat: Union[float, Iterable[float]] = None,
        x: Union[float, Iterable[float]] = None,
        y: Union[float, Iterable[float]] = None,
        unique: bool = False,
        fast: bool = True,
        npoints: int = 1,
        gridded_shape: Optional[tuple[int]] = None,
    ) -> dict[str, np.ndarray]:
        """Finds points nearest to the x-y, lon-lat points provided and returns dict of corresponding indeces.

        All Skeletons: key 'dx' (distance to nearest point in meters)

        PointSkelton: keys 'inds'
        GriddedSkeleton: keys 'inds_x' and 'inds_y'

        Set unique=True to remove any repeated points.
        Set fast=True to use UTM cartesian search for low latitudes.
        npoints can be used to find N nearest points."""

        if all([x is None for x in (x, y, lon, lat)]):
            raise ValueError("Give either x-y pair or lon-lat pair!")

        if self.core.is_cartesian():
            fast = True

        x = sanitize.force_to_iterable(x)
        y = sanitize.force_to_iterable(y)
        lon = sanitize.force_to_iterable(lon)
        lat = sanitize.force_to_iterable(lat)
        # If lon/lat is given, convert to cartesian and set grid UTM zone to match the query point
        if lon is not None and lat is not None:
            inds, dx = self._yank_using_lonlat(lon, lat, fast, npoints)
        else:
            inds, dx = self._yank_using_xy(x, y, fast, npoints)

        if unique:
            inds = np.unique(inds)

        if self.is_gridded():
            inds_x = []
            inds_y = []
            for ind in inds:
                indy, indx = np.unravel_index(ind, self.size())
                inds_x.append(indx)
                inds_y.append(indy)
            return {
                "inds_x": np.array(inds_x),
                "inds_y": np.array(inds_y),
                "dx": np.array(dx),
            }
        else:
            if gridded_shape is None:
                return {"inds": np.array(inds), "dx": np.array(dx)}
            if self.nx() != np.prod(gridded_shape):
                raise ValueError(
                    f"Number of elements in 'gridded_shape' ({gridded_shape}) does not match elements in skeleton ({self.nx()}!"
                )
            yi, xi = np.unravel_index(np.array(inds), gridded_shape)
            return {
                "inds": np.array(inds),
                "dx": np.array(dx),
                "inds_x": np.atleast_1d(xi),
                "inds_y": np.atleast_1d(yi),
            }

    def _yank_using_xy(
        self, x: np.ndarray, y: np.ndarray, fast: bool, npoints: int
    ) -> dict[str, np.ndarray]:
        """Finds the indeces of nearest points and distances if x,y coordinates are provided"""
        if self.utm.is_set():
            lat = self.utm._lat(x, y, self.utm.zone())
            lon = self.utm._lon(x, y, self.utm.zone())
        else:
            lat, lon = None, None

        inds, dx = self._yank_inds(x, y, lon, lat, self.utm.zone(), fast, npoints)
        return inds, dx

    def _yank_using_lonlat(
        self, lon: np.ndarray, lat: np.ndarray, fast: bool, npoints: int
    ) -> tuple[np.ndarray, np.ndarray]:
        """Finds the indeces of nearest points and distances if lon,lat coordinates are provided"""
        if self.core.is_cartesian():
            utm_to_use = self.utm.zone()
        else:
            utm_to_use = self.utm.optimal_utm(lon=lon, lat=lat)

        if utm_to_use[0] is not None:
            x = self.utm._x(lon=lon, lat=lat, utm=utm_to_use)
            y = self.utm._y(lon=lon, lat=lat, utm=utm_to_use)
        else:
            x, y = None, None
        inds, dx = self._yank_inds(x, y, lon, lat, utm_to_use, fast, npoints)
        return inds, dx

    def _yank_inds(
        self,
        x: np.ndarray,
        y: np.ndarray,
        lon: np.ndarray,
        lat: np.ndarray,
        utm_to_use: tuple[int, str],
        fast: bool,
        npoints: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Applies a cartesian or spherical search on given coordinates, finding nearest points and returning indeces and distances."""
        inds = []
        dx = []
        xlist, ylist = self.xy(utm=utm_to_use)
        lonlist, latlist = self.lonlat()

        if (
            x is None
        ):  # x is None e.g. when all lat are over 84 deg or no UTM zone is set
            fast = False
        elif lat is None:  # lat is None e.g. when no UTM zone is set
            fast = True

        if np.any(
            np.isnan(ylist)
        ):  # Some over 84 deg latitudes means we can't calculate shorest cartesian distance
            fast = False

        if x is not None:
            number_of_points = len(x)
        else:
            number_of_points = len(lon)

        if lat is not None:
            out_of_range_lats = np.logical_or(lat > 84, lat < -80)
        else:
            out_of_range_lats = np.full(number_of_points, False)

        for n in range(number_of_points):
            dxx, ii = None, None

            if out_of_range_lats[n] or not fast:  # Over 84 lat so using slow method
                dxx, ii = distance_funcs.min_distance(
                    lon[n], lat[n], lonlist, latlist, npoints
                )
            else:
                dxx, ii = distance_funcs.min_cartesian_distance(
                    x[n], y[n], xlist, ylist, npoints
                )

            if dxx is not None:
                inds.append(ii)
                dx.append(dxx)
        return list(itertools.chain.from_iterable(inds)), list(
            itertools.chain.from_iterable(dx)
        )

    @property
    def name(self) -> str:
        if not hasattr(self, "_name"):
            return "LonelySkeleton"
        return self._name

    @name.setter
    def name(self, new_name: str) -> None:
        if isinstance(new_name, str):
            self._name = new_name
        else:
            raise ValueError("name needs to be a string")

    def _chunk_tuple_from_dict(self, chunk_dict: dict) -> tuple[int]:
        """Determines a tuple of chunks based on a dict of coordinates and chunks"""
        chunk_list = []
        for coord in self.core.coords():
            chunk_list.append(chunk_dict.get(coord, "auto"))
        return tuple(chunk_list)

    def iterate(self, coords: Optional[list[str]] = None):
        """Return an iterator object for iterating over a list of coordinates.

        E.g. to iterates first over 'time' values, and then over 'z' values:
        for slice in skeleton.iterate(['time','z']):
            pass


        Default is the defined 'grid' coord group (including basic spatial coords), which is identical to:
        for slice in skeleton:
            pass
        """
        coords = coords or self.core.coords("grid")
        return iter(self)(coords)

    def __iter__(self):
        """Equal to calling skeleton.iterate()"""
        coords_dict = {coord: self.get(coord) for coord in self.core.coords("all")}
        return SkeletonIterator(
            coords_dict,
            self.core.coords("grid"),
            self,
        )

    def __repr__(self) -> str:
        def string_of_coords(list_of_coords) -> str:
            if not list_of_coords:
                return ""
            string = "("
            for c in list_of_coords:
                string += f"{c}, "
            string = string[:-2]
            string += ")"
            return string

        string = f"<{type(self).__name__} ({self.__class__.__base__.__name__})>\n"

        string += f"{' Coordinate groups ':-^80}" + "\n"
        string += f"{'Spatial:':12}"

        string += string_of_coords(self.core.coords("spatial")) or "*empty*"
        string += f"\n{'Grid:':12}"
        string += string_of_coords(self.core.coords("grid")) or "*empty*"
        string += f"\n{'Gridpoint:':12}"
        string += string_of_coords(self.core.coords("gridpoint")) or "*empty*"

        string += f"\n{'All:':12}"
        string += string_of_coords(self.core.coords("all")) or "*empty*"

        string += "\n" + f"{' Xarray ':-^80}" + "\n"
        string += self.ds().__repr__()

        empty_vars = self._ds_manager.empty_vars()
        empty_masks = self._ds_manager.empty_masks()

        if empty_masks or empty_vars:
            string += "\n" + f"{' Empty data ':-^80}"

            if empty_vars:
                string += "\n" + "Empty variables:"
                max_len = len(max(empty_vars, key=len))
                for var in empty_vars:
                    string += f"\n    {var:{max_len+2}}"
                    string += string_of_coords(
                        self.core.coords(self.core.coord_group(var))
                    )
                    string += f":  {self.core.default_value(var)}"
                    meta_parameter = self.core.meta_parameter(var)
                    if meta_parameter is not None:
                        string += f" [{meta_parameter.units()}]"
                        string += f" {meta_parameter.standard_name()}"

            if empty_masks:
                string += "\n" + "Empty masks:"
                max_len = len(max(empty_masks, key=len))
                for mask in empty_masks:
                    string += f"\n    {mask:{max_len+2}}"
                    string += string_of_coords(
                        self.core.coords(self.core.coord_group(mask))
                    )
                    string += f":  {bool(self.core.default_value(mask))}"

        magnitudes = self.core.magnitudes()

        if magnitudes:
            string += "\n" + f"{' Magnitudes and directions ':-^80}"
            for key in magnitudes:
                value = self.core.get(key)
                string += f"\n  {key}: magnitude of ({value.x},{value.y})"

                meta_parameter = self.core.meta_parameter(key)
                if meta_parameter is not None:
                    string += f" [{meta_parameter.units()}]"
                    string += f" {meta_parameter.standard_name()}"

        directions = self.core.directions()
        if directions:
            for key in directions:
                value = self.core.get(key)
                string += f"\n  {key}: direction of ({value.x},{value.y})"
                meta_parameter = self.core.meta_parameter(key)
                if meta_parameter is not None:
                    string += f" [{meta_parameter.units()}]"
                    string += f" {meta_parameter.standard_name()}"

        string += "\n" + "-" * 80

        return string


def _modified_name(old_name: str) -> str:
    if "Modified" in old_name:
        return old_name
    return f"Modified{old_name}"


def _determine_inds(coord_slice, all_vals):
    if coord_slice is None:
        start, stop = np.nanmin(all_vals), np.nanmax(all_vals)
    else:
        if coord_slice.step is not None:
            raise ValueError("PointSkeletons can't be sliced with a step!")
        start, stop = coord_slice.start, coord_slice.stop

    coord_inds = np.where(
        np.logical_and(
            all_vals >= start,
            all_vals <= stop,
        )
    )[0]
    return coord_inds
