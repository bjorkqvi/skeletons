import numpy as np
import xarray as xr
from .managers.dataset_manager import DatasetManager
from .managers.dask_manager import DaskManager
from .managers.reshape_manager import ReshapeManager
from .managers.dir_type_manager import DirTypeManager

# from .managers.data_sanitizer import DataSanitizer, will_grid_be_spherical_or_cartesian
from . import data_sanitizer as sanitize
from .managers.utm_manager import UTMManager
from typing import Iterable, Union, Optional
from . import distance_funcs
from .errors import DataWrongDimensionError

from typing import Iterable
from copy import deepcopy
from .decorators import add_datavar, add_magnitude
from .iter import SkeletonIterator

from geo_skeletons import dask_computations


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
            self.meta = deepcopy(self.meta)
            self.meta._coord_manager = self.core

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
        self._dir_type_manager = DirTypeManager(coord_manager=self.core)

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

    def _init_metadata(self) -> None:
        """Initialized the metadata by using availabe metadata in the GeoParameters"""
        for name in self.core.all_objects("all"):
            metavar = self.core.get(name).meta
            if metavar is not None:
                self.meta.append(metavar.meta_dict(), name)

    def add_datavar(
        self, name: str, coord_group: str = "all", default_value: float = 0.0
    ) -> None:
        """Adds a data variable to an instance of a Skeleton.

        Similar to using @add_datavar on a class, but only affects an instance."""
        self = add_datavar(
            name=name, coord_group=coord_group, default_value=default_value, append=True
        )(self)
        self._ds_manager.coord_manager = self.core
        self.meta._ds_manager = self._ds_manager

    def add_magnitude(
        self, name: str, x: str, y: str, direction: str = None, dir_type: str = None
    ) -> None:
        """Adds a magnitude to an instance of a Skeleton.

        Similar to using @add_magnitude on a class, but only affects an instance."""
        self = add_magnitude(
            name=name, x=x, y=y, direction=direction, dir_type=dir_type, append=True
        )(self)
        self._ds_manager.coord_manager = self.core
        self.meta._ds_manager = self._ds_manager

    @classmethod
    def from_ds(
        cls,
        ds: xr.Dataset,
        chunks: Optional[Union[tuple[int], str]] = None,
        **kwargs,
    ) -> "Skeleton":
        """Generats an instance of a Skeleton form an xarray Dataset.
        All coordinates must be present, but only matching data variables included.

        Missing coordinates can be provided as kwargs. If e.g. the z-variable doesn't exist in the DataSet:

        new_instance = SkeletonClass.from_ds(ds, z=[1,2,3])"""
        # Getting mandatory spatial variables
        lon, lat = ds.get("lon"), ds.get("lat")
        x, y = ds.get("x"), ds.get("y")

        if lon is not None:
            lon = lon.values
        if lat is not None:
            lat = lat.values
        if x is not None:
            x = x.values
        if y is not None:
            y = y.values

        if x is None and y is None and lon is None and lat is None:
            raise ValueError("Can't find x/y lon/lat pair in Dataset!")

        # Gather other coordinates
        coords = cls.core.coords("all")  # list(ds.coords) + list(kwargs.keys())
        additional_coords = {}
        for coord in [
            coord for coord in coords if coord not in ["inds", "lon", "lat", "x", "y"]
        ]:
            ds_val = ds.get(coord)

            provided_val = kwargs.get(coord)
            val = provided_val if provided_val is not None else ds_val

            if val is None:
                raise KeyError(
                    f"Can't find required coordinate {coord} in Dataset or in kwargs!"
                )
            else:
                if isinstance(val, xr.DataArray):
                    val = val.data
            additional_coords[coord] = val

        # Initialize Skeleton
        points = cls(x=x, y=y, lon=lon, lat=lat, chunks=chunks, **additional_coords)
        # Set data variables and masks that exist
        for data_var in points.core.non_coord_objects():
            val = ds.get(data_var)

            if val is not None:
                points.set(data_var, val)
                points.meta.set(ds.get(data_var).attrs, name=data_var)
        points.meta.set(ds.attrs)

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

    def sel(self, **kwargs) -> "Skeleton":
        """Creates a new instance by selecting only some of the wanted variables.
        e.g. new_skeleton = skeleton.sel(lon=slice(10,20))

        Calls the Xarray .sel method on the underlying DataSet"""
        return self.from_ds(self.ds().sel(**kwargs))

    def isel(self, **kwargs) -> "Skeleton":
        """Creates a new instance by selecting only some of the wanted variables.
        e.g. new_skeleton = skeleton.isel(lon=[0,1,2])

        Calls the Xarray .isel method on the underlying DataSet"""
        return self.from_ds(self.ds().isel(**kwargs))

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
        name: str,
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

        if not isinstance(name, str):
            raise TypeError(
                f"'name' must be of type 'str', not '{type(name).__name__}'!"
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

        # Masks are stored as integers
        if name in self.core.masks("all"):
            data = data.astype(int)

        if name in self.core.magnitudes("all"):
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
            )

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
        dir_data = self.get(obj.direction.name, dir_type="math")

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
        mag_data = self.get(obj.magnitude.name)

        dir_type = dir_type or obj.dir_type

        data = self._dir_type_manager.convert_to_math_dir(data, dir_type)

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
    ) -> None:
        """Sets a data variable to the underlying dataset.

        Data needs to be exactly right shape.

        Triggers setting metadata of the variable and possible connected masks."""
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

        if dir_type not in ["to", "from", "math", None]:
            raise ValueError(
                f"'dir_type' needs to be 'to', 'from' or 'math' (or None), not {dir_type}"
            )

        if name in self.core.magnitudes():
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
        data = self._dir_type_manager.compute_math_direction(x_data, y_data)
        data = self._dir_type_manager.convert_from_math_dir(data, dir_type=dir_type)

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

        data = self._dir_type_manager.compute_magnitude(x_data, y_data)

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

        set_dir_type = self._dir_type_manager.get_dir_type(name)
        dir_type = dir_type or set_dir_type
        data = self._dir_type_manager.convert(
            data, in_type=set_dir_type, out_type=dir_type
        )
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

    def ds(self) -> Union[xr.Dataset, None]:
        """Returns the underlying Xarray Dataset. None if dosen't exist."""
        if not hasattr(self, "_ds_manager"):
            return None
        return self._ds_manager.ds()

    def find_cf(self, standard_name: str) -> list[str]:
        """Finds the variable names that have the given standard name"""
        names = []

        for name in self.core.all_objects():
            obj = self.core.get(name)
            if obj.meta is None:
                continue
            if (
                obj.meta.standard_name() == standard_name
                or obj.meta.standard_name(alias=True) == standard_name
            ):
                names.append(obj.name)

        return names

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
        self, coord: str, native: bool = False, strict=False
    ) -> tuple[float, float]:
        """Min and max values of x. Conversion made for sperical grids."""
        if coord not in ["x", "y", "lon", "lat"]:
            print("coord need to be 'x', 'y', 'lon' or 'lat'.")
            return

        if coord in ["x", "y"]:
            x, y = self.xy(native=native, strict=strict)
        else:
            x, y = self.lonlat(native=native, strict=strict)

        if coord in ["x", "lon"]:
            val = x
        else:
            val = y

        if val is None:
            return (None, None)

        return np.min(val), np.max(val)

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

        return (
            max(self.x(native=native, suppress_warning=True))
            - min(self.x(native=native, suppress_warning=True))
        ) / (self.nx() - 1)

    def dy(self, native: bool = False, strict: bool = False) -> float:
        """Mean grid spacing of the y vector. Conversion made for spherical grids."""
        if not self.core.is_cartesian() and strict and (not native):
            return None

        if self.ny() == 1:
            return 0.0

        return (
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

        return (max(lon) - min(lon)) / (self.nx() - 1)

    def dlat(self, native: bool = False, strict: bool = False):
        """Mean grid spacing of the latitude vector. Conversion made for
        cartesian grids."""
        if self.ny() == 1:
            return 0.0
        lat = self.lat(native=native, strict=strict, suppress_warning=True)
        if lat is None:
            return None

        return (max(lat) - min(lat)) / (self.ny() - 1)

    def yank_point(
        self,
        lon: Union[float, Iterable[float]] = None,
        lat: Union[float, Iterable[float]] = None,
        x: Union[float, Iterable[float]] = None,
        y: Union[float, Iterable[float]] = None,
        unique: bool = False,
        fast: bool = False,
    ) -> dict[str, np.ndarray]:
        """Finds points nearest to the x-y, lon-lat points provided and returns dict of corresponding indeces.

        All Skeletons: key 'dx' (distance to nearest point in km)

        PointSkelton: keys 'inds'
        GriddedSkeleton: keys 'inds_x' and 'inds_y'

        Set unique=True to remove any repeated points.
        Set fast=True to use UTM cartesian search for low latitudes."""

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
            inds, dx = self._yank_using_lonlat(lon, lat, fast)
        else:
            inds, dx = self._yank_using_xy(x, y, fast)

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
            return {"inds": np.array(inds), "dx": np.array(dx)}

    def _yank_using_xy(
        self, x: np.ndarray, y: np.ndarray, fast: bool
    ) -> dict[str, np.ndarray]:
        """Finds the indeces of nearest points and distances if x,y coordinates are provided"""
        if self.utm.is_set():
            lat = self.utm._lat(x, y)
            lon = self.utm._lon(x, y)
        else:
            lat, lon = None, None

        inds, dx = self._yank_inds(x, y, lon, lat, self.utm.zone(), fast)
        return inds, dx

    def _yank_using_lonlat(
        self, lon: np.ndarray, lat: np.ndarray, fast: bool
    ) -> tuple[np.ndarray, np.ndarray]:
        """Finds the indeces of nearest points and distances if lon,lat coordinates are provided"""
        if self.core.is_cartesian():
            utm_to_use = self.utm.zone()
        else:
            utm_to_use = self.utm.optimal_utm(lon=lon, lat=lat)

        x = self.utm._x(lon=lon, lat=lat, utm=utm_to_use)
        y = self.utm._y(lon=lon, lat=lat, utm=utm_to_use)
        inds, dx = self._yank_inds(x, y, lon, lat, utm_to_use, fast)
        return inds, dx

    def _yank_inds(
        self,
        x: np.ndarray,
        y: np.ndarray,
        lon: np.ndarray,
        lat: np.ndarray,
        utm_to_use: tuple[int, str],
        fast: bool,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Applies a cartesian or spherical search on given coordinates, finding nearest points and returning indeces and distances."""
        inds = []
        dx = []

        xlist, ylist = self.xy(utm=utm_to_use)
        lonlist, latlist = self.lonlat()
        if lat is not None:
            posmask = np.logical_or(lat > 84, lat < -84)
        else:
            fast = True

        for (
            n,
            (xx, yy),
        ) in enumerate(zip(x, y)):
            dxx, ii = None, None
            if lat is None:  # No UTM zone set so only option to use cartesian check-up
                dxx, ii = distance_funcs.min_cartesian_distance(xx, yy, xlist, ylist)
            elif posmask[n]:  # Over 84 lat so using slow method even if fast requested
                if latlist is not None:
                    dxx, ii = distance_funcs.min_distance(
                        lon[n], lat[n], lonlist, latlist
                    )
            elif fast:
                dxx, ii = distance_funcs.min_cartesian_distance(xx, yy, xlist, ylist)
            else:
                if latlist is not None:
                    dxx, ii = distance_funcs.min_distance(
                        lon[n], lat[n], lonlist, latlist
                    )
            if dxx is not None:
                inds.append(ii)
                dx.append(dxx)
        return inds, dx

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
                        string += f" [{meta_parameter.unit()}]"
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
                    string += f" [{meta_parameter.unit()}]"
                    string += f" {meta_parameter.standard_name()}"

        directions = self.core.directions()
        if directions:
            for key in directions:
                value = self.core.get(key)
                string += f"\n  {key}: direction of ({value.x},{value.y})"
                meta_parameter = self.core.meta_parameter(key)
                if meta_parameter is not None:
                    string += f" [{meta_parameter.unit()}]"
                    string += f" {meta_parameter.standard_name()}"

        string += "\n" + "-" * 80

        return string
