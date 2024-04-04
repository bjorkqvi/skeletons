import numpy as np
import xarray as xr
import utm as utm_module
from copy import copy
from .managers.dataset_manager import DatasetManager
from .managers.dask_manager import DaskManager
from typing import Iterable, Union
from .aux_funcs import distance_funcs, array_funcs, utm_funcs
from .errors import DataWrongDimensionError

from typing import Iterable
import dask.array as da
from copy import deepcopy
from .decorators import add_datavar
from types import MethodType
from .iter import SkeletonIterator

DEFAULT_UTM = (33, "W")


class Skeleton:
    """Contains methods and data of the spatial x,y / lon, lat coordinates and
    makes possible conversions between them.

    Keeps track of the native structure of the grid (cartesian UTM / sperical).
    """

    def __init__(
        self, x=None, y=None, lon=None, lat=None, name: str = "LonelySkeleton", **kwargs
    ) -> None:
        self.name = name
        self.dask = True
        self.chunks = "auto"
        self._init_structure(x, y, lon, lat, **kwargs)
        self.data_vars = MethodType(_data_vars, self)

    def add_datavar(
        self, name: str, coords: str = "all", default_value: float = 0.0
    ) -> None:
        self = add_datavar(
            name=name, coords=coords, default_value=default_value, append=True
        )(self)

    @classmethod
    def from_ds(cls, ds: xr.Dataset, **kwargs):
        """Generats a PointSkeleton from an xarray Dataset. All coordinates must be present, but only matching data variables included.

        Missing coordinates can be provided as kwargs."""
        coords = list(ds.coords) + list(kwargs.keys())

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
        additional_coords = {}
        for coord in [
            coord for coord in coords if coord not in ["inds", "lon", "lat", "x", "y"]
        ]:
            ds_val = ds.get(coord)
            if ds_val is not None:
                ds_val = ds_val.values
            provided_val = kwargs.get(coord)

            val = provided_val
            if val is None:
                val = ds_val
            # val = provided_val or ds_val
            if val is None:
                raise ValueError(
                    f"Can't find required coordinate {coord} in Dataset or in kwargs!"
                )
            additional_coords[coord] = val

        # Initialize Skeleton
        points = cls(x=x, y=y, lon=lon, lat=lat, **additional_coords)
        # Set data variables and masks that exist
        for data_var in points.data_vars():
            val = ds.get(data_var)
            if val is not None:
                points.set(data_var, val)
                points.set_metadata(ds.get(data_var).attrs, data_array_name=data_var)
        points.set_metadata(ds.attrs)

        return points

    def _init_structure(self, x=None, y=None, lon=None, lat=None, **kwargs) -> None:
        """Determines grid type (Cartesian/Spherical), generates a DatasetManager
        and initializes the Xarray dataset within the DatasetManager.

        The initial coordinates and variables are read from the method of the
        subclass (e.g. PointSkeleton)
        """

        # Don't want to alter the CoordManager of the class
        self._coord_manager = deepcopy(self._coord_manager)
        self._coord_manager.initial_state = False

        x, y, lon, lat, kwargs = array_funcs.sanitize_input(
            x, y, lon, lat, self.is_gridded(), **kwargs
        )

        x_str, y_str, xvec, yvec = utm_funcs.will_grid_be_spherical_or_cartesian(
            x, y, lon, lat
        )
        self.x_str = x_str
        self.y_str = y_str

        # Reset initial coordinates and data variables (default are 'x','y' but might now be 'lon', 'lat')
        self._coord_manager.set_initial_coords(
            self._initial_coords(spherical=(x_str == "lon"))
        )
        self._coord_manager.set_initial_vars(
            self._initial_vars(spherical=(x_str == "lon"))
        )

        # The manager contains the Xarray Dataset
        if not self._structure_initialized():
            self._ds_manager = DatasetManager(self._coord_manager)

        self._ds_manager.create_structure(xvec, yvec, self.x_str, self.y_str, **kwargs)
        self.set_utm(silent=True)

        # self._reset_masks()
        # self._reset_datavars()

    def absorb(self, skeleton_to_absorb, dim: str) -> None:
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

    @classmethod
    def data_vars(cls) -> None:
        return list(cls._coord_manager.added_vars().keys())

    def coords(self, coords: str = "all") -> list[str]:
        """Returns a list of the coordinates from the Dataset.

        'all' [default]: all coordinates in the Dataset
        'spatial': Dataset coordinates from the Skeleton (x, y, lon, lat, inds)
        'grid': coordinates for the grid (spatial and e.g. z, time)
        'gridpoint': coordinates for a grid point (e.g. frequency, direcion or time)
        """
        return self._coord_manager.coords(coords)

    def coord_group(self, var: str) -> str:
        """Returns the coordinate group that a variable/mask is defined over. The coordinates can then be retrived using the group by the method .coords()"""
        var_coords = self._coord_manager.added_vars().get(var)
        mask_coords = self._coord_manager.added_masks().get(var)
        if mask_coords is None:
            mask_name = self._coord_manager.opposite_masks().get(var)
            mask_coords = self._coord_manager.added_masks().get(mask_name)

        mag = self._coord_manager.magnitudes.get(var)
        if mag is not None:
            mag_coords = self._coord_manager.added_vars().get(mag["x"])
        else:
            mag_coords = None

        dir = self._coord_manager.directions.get(var)
        if dir is not None:
            dir_coords = self._coord_manager.added_vars().get(dir["x"])
        else:
            dir_coords = None

        coord_group = var_coords or mask_coords or mag_coords or dir_coords
        if coord_group is None:
            raise KeyError(f"Cannot find the data {var}!")

        return coord_group

    def coords_dict(
        self, type: str = "all", data_array: bool = False, **kwargs
    ) -> dict:
        """Return variable dictionary of the Dataset.

        'all': all coordinates in the Dataset
        'spatial': Dataset coordinates from the Skeleton (x, y, lon, lat, inds)
        'grid': coordinates for the grid (e.g. z, time)
        'gridpoint': coordinates for a grid point (e.g. frequency, direcion or time)
        """
        return {
            coord: self.get(coord, data_array=data_array, **kwargs)
            for coord in self.coords(type)
        }

    def sel(self, **kwargs):
        return self.from_ds(self.ds().sel(**kwargs))

    def isel(self, **kwargs):
        return self.from_ds(self.ds().isel(**kwargs))

    def insert(self, name: str, data: np.ndarray, **kwargs) -> None:
        """Inserts a slice of data into the Skeleton.

        If data named 'geodata' has shape dimension ('time', 'inds', 'threshold') and shape (57, 10, 3), then
        data_slice having the threshold=0.4 and time='2023-11-08 12:00:00' having shape=(10,) can be inserted by using the values:

        .insert(name='geodata', data=data_slice, time='2023-11-08 12:00:00', threshold=0.4)
        """
        dims = self.ds().dims
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

        .ind_insert(name='geodata', data=data_slice, time=0, threshold=0)"""

        dims = self.ds().dims
        index_list = list(np.arange(len(dims)))
        for n, dim in enumerate(dims):
            var = self.get(dim)
            if var is None:
                raise KeyError(f"No coordinate {dim} exists!")
            ind = kwargs.get(dim, slice(len(var)))
            index_list[n] = ind

        old_data = self.get(name)
        N = len(old_data.shape)
        data_str = "old_data["
        for n in range(N):
            data_str += f"{index_list[n]},"
        data_str = data_str[:-1]
        data_str += "] = data"
        exec(data_str)
        self.set(name, old_data)
        return

    def set(
        self,
        name: str,
        data=None,
        allow_reshape: bool = True,
        allow_transpose: bool = False,
        coords: list[str] = None,
        silent: bool = True,
        chunks: tuple | str = None,
    ) -> None:
        """Sets the data using the following logic:

        Any numpy array is converted to a dask-array, unless dask-mode is deactivated with .deactivate_dask().
        If keyword 'chunks' is set, then conversion to dask is always done.

        If given data is a dask array, then it is never rechunked, but used as is.

        Data is assumed to be in the right dimension, but can also be reshaped:

        1) If 'coords' (e.g. ['freq',' inds']) is given, then data is reshaped assuming data is in that order.
        2) If data is a DataArray, then 'coords' is set using the information in the DataArray.
        3) If data has any trivial dimensions, then those are squeezed.
        4) If data is missing any trivial dimension, then those are expanded.
        5) If data along non-trivial dimensions is two-dimensional, then a transpose is attemted.

        NB! For 1), only non-trivial dimensions need to be identified
        """

        def data_is_dask(data) -> bool:
            """Checks if a data array is a dask array"""
            return hasattr(data, "chunks")

        def dask_me(data, chunks):
            """Convert a numpy array to a dask array if needed and wanted"""
            # Don't do anything if array is already dask
            if data_is_dask(data):
                if chunks is not None:
                    data = data.rechunk(chunks)
                return data

            # If dask-mode is deactivated and no chunks are explicitly given, then keep the numpy array
            if chunks is None and not self.dask:
                return data

            # Otherwise make sure to convert to a dask array
            chunks = chunks or self.chunks
            chunks = chunks or "auto"
            return da.from_array(data, chunks=chunks)

        def transpose_me(data, coord_order):
            if len(data.shape) > len(coord_order):
                data = data.squeeze()
            if data_is_dask(data):
                return da.transpose(data, coord_order)
            else:
                return np.transpose(data, coord_order)

        def transpose_given_data(data):
            """Transposes given data if it is a two dimensional transpose of the wanted size after removing all trivial dimension.

            If sizes match, it just squeezes the data."""
            # Check if the base data (ignoring any trivial dimensions is the right (or possibly transposed) dimensions
            expected_squeeezed_shape = self.size(coord_type, squeeze=True)
            actual_squeezed_shape = data.squeeze().shape

            if expected_squeeezed_shape == actual_squeezed_shape:
                return data.squeeze()

            # If data is not 2D and doesn't match, we don't want to try to reshape
            if len(actual_squeezed_shape) != 2 or len(expected_squeeezed_shape) != 2:
                return None

            # Is the squeezed shape a transpose of the expected squeezed shape?
            if (
                tuple(np.flip(actual_squeezed_shape)) == expected_squeeezed_shape
                and allow_transpose
            ):
                return data.squeeze().T

        coord_type = self.coord_group(name)

        metadata = self.metadata()

        if data is None:
            data = self.get(name, empty=True, squeeze=False)

        # Masks are stored as integers
        if name[-5:] == "_mask":
            data = data.astype(int)

        # If a DataArray is given, then read the dimensions from there if not explicitly provided in a keyword
        if isinstance(data, xr.DataArray):
            coords = coords or list(data.dims)
            data = data.data

        # 1 & 2)
        # If the coordinates of the data are explicilty given as a coord-list or thorugh a DataArray, try to reshape to those
        if coords is not None:
            allow_reshape = True
            data_coordinates = self.coords(self.coord_group(name))
            # Check that we don't do trivial reshape
            if data_coordinates == coords:
                allow_reshape = False

            # Create a list of shapes based on the given coordinates
            coord_order = [coords.index(c) for c in data_coordinates if c in coords]
            if allow_reshape:
                if not silent:
                    print(
                        f"Reshaping data {data.shape} -> {transpose_me(data, tuple(coord_order)).shape}: {coords} -> {data_coordinates}"
                    )
                data = transpose_me(data, tuple(coord_order))

        # First try to set the data here
        data = dask_me(data, chunks)
        try:
            self._ds_manager.set(data=data, data_name=name, coords=coord_type)
            self.set_metadata(metadata)
            return
        except DataWrongDimensionError as data_error:
            if not allow_reshape:
                raise data_error

            original_data_shape = data.shape
            data = transpose_given_data(data)
            if data is None:
                raise data_error

        # We now know that the data is of right size if we ignore trivial dimensions
        # Data also has all trivial dimensions squeezed by now

        if not silent:
            print(f"Size of {name} does not match size of {type(self).__name__}...")

        expected_shape = self.size(coord_type)
        actual_shape = data.shape

        if expected_shape != actual_shape:
            trivial_places = tuple(np.where(np.array(expected_shape) == 1)[0])
            if data_is_dask(data):
                data = da.expand_dims(data, axis=trivial_places)
            else:
                data = np.expand_dims(data, axis=trivial_places)

        if not silent:
            print(f"Reshaping data {original_data_shape} -> {data.shape}...")

        self._ds_manager.set(data=data, data_name=name, coords=coord_type)
        self.set_metadata(metadata)
        return

    def get(
        self,
        name,
        empty=False,
        data_array: bool = False,
        squeeze: bool = True,
        boolean_mask: bool = False,
        dask: bool = None,
        **kwargs,
    ):
        """Gets a mask or data variable.

        Masks
        You can also request empty masks that will be return even if data doesn't exist.
        """
        if not self._structure_initialized():
            return None

        data = self._ds_manager.get(
            name, empty=empty, chunks=self.chunks or "auto", **kwargs
        )

        if name in self.coords("all"):
            dask = False

        if not isinstance(data, xr.DataArray):
            return None

        if name[-5:] == "_mask":
            boolean_mask = True

        if boolean_mask or squeeze:
            data = data.copy()

        if boolean_mask:
            data = data.astype(bool)

        if squeeze and data.shape != (1,):  # Don't squeeze out last dimension
            data = data.squeeze(drop=True)

        # Use dask mode default if not explicitly overridden
        if dask is None:
            dask = self.dask

        dask_manager = DaskManager(self.chunks)

        if dask:
            data = dask_manager.dask_me(data)
        else:
            data = dask_manager.undask_me(data)

        if not data_array:
            data = data.data

        return data

    def is_initialized(self) -> bool:
        return hasattr(self, "x_str") and hasattr(self, "y_str")

    def is_cartesian(self) -> bool:
        """Checks if the grid is cartesian (True) or spherical (False)."""
        if not self._structure_initialized():
            return False
        if self.x_str == "x" and self.y_str == "y":
            return True
        elif self.x_str == "lon" and self.y_str == "lat":
            return False
        raise Exception(
            f"Expected x- and y string to be either 'x' and 'y' or 'lon' and 'lat', but they were {self.x_str} and {self.y_str}"
        )

    def set_utm(self, utm_zone: tuple[int, str] = None, silent: bool = False):
        """Set UTM zone and number to be used for cartesian coordinates.

        If not given for a spherical grid, they will be deduced.

        If not given for a cartesian grid, will be set to default (33, 'W')
        """

        if utm_zone is None:
            if self.is_cartesian():
                zone_number, zone_letter = DEFAULT_UTM
            else:
                lon, lat = self.lonlat()
                # *** utm.error.OutOfRangeError: latitude out of range (must be between 80 deg S and 84 deg N)
                mask = np.logical_and(lat < 84, lat > -80)
                # raise OutOfRangeError('longitude out of range (must be between 180 deg W and 180 deg E)')

                lat, lon = lat[mask], lon[mask]

                # *** ValueError: latitudes must all have the same sign
                if len(lat[lat >= 0]) > len(lat[lat < 0]):
                    lat, lon = lat[lat >= 0], lon[lat >= 0]
                else:
                    lat, lon = lat[lat < 0], lon[lat < 0]

                __, __, zone_number, zone_letter = utm_module.from_latlon(lat, lon)
        else:
            zone_number, zone_letter = utm_zone

        if isinstance(zone_number, int) or isinstance(zone_number, float):
            number = copy(int(zone_number))
        else:
            raise ValueError("zone_number needs to be an integer")

        if isinstance(zone_letter, str):
            letter = copy(zone_letter)
        else:
            raise ValueError("zone_number needs to be an integer")

        if not utm_funcs.valid_utm_zone((number, letter)):
            raise ValueError(f"({number}, {letter}) is not a valid UTM zone!")

        self._zone_number = number
        self._zone_letter = letter
        if self.is_cartesian():
            self.set_metadata({"utm_zone": f"{number:02.0f}{letter}"}, append=True)

        if not silent:
            print(f"Setting UTM ({number}, {letter})")

    def utm(self) -> tuple[int, str]:
        """Returns UTM zone number and letter. Returns 33, 'W' as default
        value if it hasn't been set by the user."""
        zone_number, zone_letter = DEFAULT_UTM

        if hasattr(self, "_zone_number"):
            zone_number = self._zone_number
        if hasattr(self, "_zone_letter"):
            zone_letter = self._zone_letter
        return zone_number, zone_letter

    def ds(self):
        if not self._structure_initialized():
            return None
        return self._ds_manager.ds()

    def size(self, coords: str = "all", squeeze: bool = False, **kwargs) -> tuple[int]:
        """Returns the size of the Dataset.

        'all' [default]: size of entire Dataset
        'spatial': size over coordinates from the Skeleton (x, y, lon, lat, inds)
        'grid': size over coordinates for the grid (e.g. z, time) ans the spatial coordinates
        'gridpoint': size over coordinates for a grid point (e.g. frequency, direcion or time)
        """

        if not self._structure_initialized():
            return None

        if coords not in ["all", "spatial", "grid", "gridpoint"]:
            raise KeyError(
                f"coords should be 'all', 'spatial', 'grid' or 'gridpoint', not {coords}!"
            )

        size = self._ds_manager.coords_to_size(self.coords(coords), **kwargs)

        if squeeze:
            size = tuple([s for s in size if s > 1])
        return size

    def shape(self, var, squeeze: bool = False, **kwargs) -> tuple[int]:
        """Returns the size of one specific data variable"""
        coords = self.coord_group(var)
        return self.size(coords=coords, squeeze=squeeze, **kwargs)

    def inds(self, **kwargs) -> np.ndarray:
        if not self._structure_initialized():
            return None
        inds = self._ds_manager.get("inds", **kwargs)
        if inds is None:
            return None
        vals = inds.values.copy()
        if vals.shape == ():
            vals = vals.reshape(1)[0]
        return vals

    def x(
        self,
        native: bool = False,
        strict: bool = False,
        normalize: bool = False,
        utm: tuple[int, str] = None,
        **kwargs,
    ) -> np.ndarray:
        """Returns the cartesian x-coordinate.

        If the grid is spherical, a conversion to UTM coordinates is made based on the medain latitude.

        If native=True, then longitudes are returned for spherical grids instead
        If strict=True, then None is returned if grid is sperical

        native=True overrides strict=True for spherical grids

        Give utm to get cartesian coordinates in specific utm system. Otherwise defaults to the one set for the grid.
        """

        if not self._structure_initialized():
            return None

        if not self.is_cartesian() and native:
            return self.lon(**kwargs)

        if not self.is_cartesian() and strict:
            return None

        if self.is_cartesian() and (self.utm() == utm or utm is None):
            x = self._ds_manager.get("x", **kwargs).values.copy()
            if normalize:
                x = x - min(x)
            return x

        if utm is None:
            number, letter = self.utm()
        else:
            number, letter = utm

        if (
            self.is_gridded()
        ):  # This will rotate the grid, but is best estimate to keep it strucutred
            lat = np.median(self.lat(**kwargs))
            # print(
            #    "Regridding spherical grid to cartesian coordinates. This will cause a rotation!"
            # )
            x, __, __, __ = utm_module.from_latlon(
                lat,
                self.lon(**kwargs),
                force_zone_number=number,
                force_zone_letter=letter,
            )
        else:
            lat = self.lat(**kwargs)
            lat = utm_funcs.cap_lat_for_utm(lat)

            posmask = lat >= 0
            negmask = lat < 0
            x = np.zeros(len(lat))
            if np.any(posmask):
                x[posmask], __, __, __ = utm_module.from_latlon(
                    lat[posmask],
                    self.lon(**kwargs)[posmask],
                    force_zone_number=number,
                    force_zone_letter=letter,
                )
            if np.any(negmask):
                x[negmask], __, __, __ = utm_module.from_latlon(
                    -lat[negmask],
                    self.lon(**kwargs)[negmask],
                    force_zone_number=number,
                    force_zone_letter=letter,
                )

        if normalize:
            x = x - min(x)

        return x

    def y(
        self,
        native: bool = False,
        strict: bool = False,
        normalize: bool = False,
        utm: tuple[int, str] = None,
        **kwargs,
    ) -> np.ndarray:
        """Returns the cartesian y-coordinate.

        If the grid is spherical, a conversion to UTM coordinates is made based on the medain latitude.

        If native=True, then latitudes are returned for spherical grids instead
        If strict=True, then None is returned if grid is sperical

        native=True overrides strict=True for spherical grids

        Give utm to get cartesian coordinates in specific utm system. Otherwise defaults to the one set for the grid.
        """

        if not self._structure_initialized():
            return None

        if not self.is_cartesian() and native:
            return self.lat(**kwargs)

        if not self.is_cartesian() and strict:
            return None

        if self.is_cartesian() and (self.utm() == utm or utm is None):
            y = self._ds_manager.get("y", **kwargs).values.copy()
            if normalize:
                y = y - min(y)
            return y

        if utm is None:
            number, letter = self.utm()
        else:
            number, letter = utm
        posmask = self.lat(**kwargs) >= 0
        negmask = self.lat(**kwargs) < 0
        if (
            self.is_gridded()
        ):  # This will rotate the grid, but is best estimate to keep it strucutred
            lon = np.median(self.lon(**kwargs))
            # print(
            #    "Regridding spherical grid to cartesian coordinates. This will cause a rotation!"
            # )
            y = np.zeros(len(self.lat(**kwargs)))
            if np.any(posmask):
                _, y[posmask], __, __ = utm_module.from_latlon(
                    self.lat(**kwargs)[posmask],
                    lon,
                    force_zone_number=number,
                    force_zone_letter=letter,
                )
            if np.any(negmask):
                _, y[negmask], __, __ = utm_module.from_latlon(
                    -self.lat(**kwargs)[negmask],
                    lon,
                    force_zone_number=number,
                    force_zone_letter=letter,
                )
                y[negmask] = -y[negmask]
        else:
            lat = utm_funcs.cap_lat_for_utm(self.lat(**kwargs))
            y = np.zeros(len(self.lat(**kwargs)))
            if np.any(posmask):
                _, y[posmask], __, __ = utm_module.from_latlon(
                    lat[posmask],
                    self.lon(**kwargs)[posmask],
                    force_zone_number=number,
                    force_zone_letter=letter,
                )
            if np.any(negmask):
                _, y[negmask], __, __ = utm_module.from_latlon(
                    -lat[negmask],
                    self.lon(**kwargs)[negmask],
                    force_zone_number=number,
                    force_zone_letter=letter,
                )
                y[negmask] = -y[negmask]

        if normalize:
            y = y - min(y)

        return y

    def lon(self, native: bool = False, strict=False, **kwargs) -> np.ndarray:
        """Returns the spherical lon-coordinate.

        If the grid is cartesian, a conversion from UTM coordinates is made based on the medain y-coordinate.

        If native=True, then x-coordinatites are returned for cartesian grids instead
        If strict=True, then None is returned if grid is cartesian

        native=True overrides strict=True for cartesian grids
        """
        if not self._structure_initialized():
            return None

        if self.is_cartesian() and native:
            return self.x(**kwargs)

        if self.is_cartesian() and strict:
            return None

        if self.is_cartesian():
            if (
                self.is_gridded()
            ):  # This will rotate the grid, but is best estimate to keep it strucutred
                y = np.median(self.y(**kwargs))
                # print(
                #    "Regridding cartesian grid to spherical coordinates. This will cause a rotation!"
                # )
            else:
                y = self.y(**kwargs)
            number, letter = self.utm()
            __, lon = utm_module.to_latlon(
                self.x(**kwargs),
                np.mod(y, 10_000_000),
                zone_number=number,
                zone_letter=letter,
                strict=False,
            )

            return lon
        return self._ds_manager.get("lon", **kwargs).values.copy()

    def lat(self, native: bool = False, strict=False, **kwargs) -> np.ndarray:
        """Returns the spherical lat-coordinate.

        If the grid is cartesian, a conversion from UTM coordinates is made based on the medain y-coordinate.

        If native=True, then y-coordinatites are returned for cartesian grids instead
        If strict=True, then None is returned if grid is cartesian

        native=True overrides strict=True for cartesian grids
        """
        if not self._structure_initialized():
            return None

        if self.is_cartesian() and native:
            return self.y(**kwargs)

        if self.is_cartesian() and strict:
            return None

        if self.is_cartesian():
            if (
                self.is_gridded()
            ):  # This will rotate the grid, but is best estimate to keep it strucutred
                x = np.median(self.x(**kwargs))
                # print(
                #    "Regridding cartesian grid to spherical coordinates. This will cause a rotation!"
                # )
            else:
                x = self.x(**kwargs)
            number, letter = self.utm()
            lat, __ = utm_module.to_latlon(
                x,
                np.mod(self.y(**kwargs), 10_000_000),
                zone_number=number,
                zone_letter=letter,
                strict=False,
            )
            return lat

        return self._ds_manager.get("lat", **kwargs).values.copy()

    def edges(
        self, coord: str, native: bool = False, strict=False
    ) -> tuple[float, float]:
        """Min and max values of x. Conversion made for sperical grids."""
        if not self._structure_initialized() or self._skeleton_empty():
            return (None, None)

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
        if not self._structure_initialized() or self._skeleton_empty():
            return 0
        return len(self.x(native=True))

    def ny(self):
        """Length of y/lat-vector."""
        if not self._structure_initialized() or self._skeleton_empty():
            return 0
        return len(self.y(native=True))

    def dx(self, native: bool = False, strict: bool = False):
        """Mean grid spacing of the x vector. Conversion made for
        spherical grids."""
        if not self._structure_initialized() or self._skeleton_empty():
            return None

        if not self.is_cartesian() and strict and (not native):
            return None

        if self.nx() == 1:
            return 0.0

        return (max(self.x(native=native)) - min(self.x(native=native))) / (
            self.nx() - 1
        )

    def dy(self, native: bool = False, strict: bool = False):
        """Mean grid spacing of the y vector. Conversion made for
        spherical grids."""
        if not self._structure_initialized() or self._skeleton_empty():
            return None

        if not self.is_cartesian() and strict and (not native):
            return None

        if self.ny() == 1:
            return 0.0

        return (max(self.y(native=native)) - min(self.y(native=native))) / (
            self.ny() - 1
        )

    def dlon(self, native: bool = False, strict: bool = False):
        """Mean grid spacing of the longitude vector. Conversion made for
        cartesian grids."""
        if not self._structure_initialized() or self._skeleton_empty():
            return None

        if self.is_cartesian() and strict and (not native):
            return None
        if self.nx() == 1:
            return 0.0

        return (max(self.lon(native=native)) - min(self.lon(native=native))) / (
            self.nx() - 1
        )

    def dlat(self, native: bool = False, strict: bool = False):
        """Mean grid spacing of the latitude vector. Conversion made for
        cartesian grids."""
        if not self._structure_initialized() or self._skeleton_empty():
            return None

        if self.is_cartesian() and strict and (not native):
            return None
        if self.ny() == 1:
            return 0.0

        return (max(self.lat(native=native)) - min(self.lat(native=native))) / (
            self.ny() - 1
        )

    def yank_point(
        self,
        lon: Union[float, Iterable[float]] = None,
        lat: Union[float, Iterable[float]] = None,
        x: Union[float, Iterable[float]] = None,
        y: Union[float, Iterable[float]] = None,
        unique: bool = False,
        fast: bool = False,
    ) -> dict:
        """Finds points nearest to the x-y, lon-lat points provided and returns dict of corresponding indeces.

        All Skeletons: key 'dx' (distance to nearest point in km)

        PointSkelton: keys 'inds'
        GriddedSkeleton: keys 'inds_x' and 'inds_y'

        Set unique=True to remove any repeated points.
        Set fast=True to use UTM casrtesian search for low latitudes."""

        if self.is_cartesian():
            fast = True

        # If lon/lat is given, convert to cartesian and set grid UTM zone to match the query point
        x = array_funcs.force_to_iterable(x)
        y = array_funcs.force_to_iterable(y)
        lon = array_funcs.force_to_iterable(lon)
        lat = array_funcs.force_to_iterable(lat)

        if all([x is None for x in (x, y, lon, lat)]):
            raise ValueError("Give either x-y pair or lon-lat pair!")

        orig_zone = self.utm()
        if lon is not None and lat is not None:
            if self.is_cartesian():
                x, y, __, __ = utm_module.from_latlon(
                    lat,
                    lon,
                    force_zone_number=orig_zone[0],
                    force_zone_letter=orig_zone[1],
                )
            else:
                x, y, zone_number, zone_letter = utm_module.from_latlon(lat, lon)
                self.set_utm((zone_number, zone_letter), silent=True)
        else:
            lat, lon = utm_module.to_latlon(
                x,
                y,
                zone_number=orig_zone[0],
                zone_letter=orig_zone[1],
                strict=False,
            )

        posmask = np.logical_or(lat > 84, lat < -84)
        inds = []
        dx = []

        xlist, ylist = self.xy()
        lonlist, latlist = self.lonlat()
        for xx, yy, llon, llat, mask in zip(x, y, lon, lat, posmask):
            if mask or not fast:
                dxx, ii = distance_funcs.min_distance(
                    llon, llat, lonlist, latlist
                )  # Slower, but works for high/low latitudes and is exact
            else:
                dxx, ii = distance_funcs.min_cartesian_distance(xx, yy, xlist, ylist)
            inds.append(ii)
            dx.append(dxx)
        self.set_utm(orig_zone, silent=True)  # Reset UTM zone

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

    def metadata(self) -> dict:
        """Return metadata of the dataset:"""
        if not self._structure_initialized():
            return None
        return self.ds().attrs.copy()

    def set_metadata(
        self, metadata: dict, append=False, data_array_name: str = None
    ) -> None:
        if not self._structure_initialized():
            return None
        if append:
            old_metadata = self.metadata()
            old_metadata.update(metadata)
            metadata = old_metadata
        self._ds_manager.set_attrs(metadata, data_array_name)

    def masks(self):
        mask_list = []
        for var in list(self.ds().data_vars):
            if var[-5:] == "_mask":
                mask_list.append(var)
        return mask_list

    def activate_dask(
        self, chunks="auto", primary_dim: str = None, rechunk: bool = True
    ) -> None:
        self.dask = True
        self.chunks = chunks
        if rechunk:
            self.rechunk(chunks, primary_dim)

    def deactivate_dask(self, unchunk: bool = False) -> None:
        """Deactivates the use of dask, meaning:

        1) Data will not be converted to dask-arrays when set, unless chunks provided
        2) Data will be converted from dask-arrays to numpy arrays when get
        3) All data will be converted to numpy arrays if unchunk=True"""
        self.dask = False
        self.chunks = None
        if unchunk:
            for var in self.data_vars():
                data = self.get(var)
                if data is not None:
                    if hasattr(data, "chunks"):
                        data = data.compute()
                    self.set(var, data)

    def rechunk(
        self, chunks: tuple | dict | str = "auto", primary_dim: str | list[str] = None
    ):
        if primary_dim:
            if isinstance(primary_dim, str):
                primary_dim = [primary_dim]
            chunks = {}
            for dim in primary_dim:
                chunks[dim] = len(self.get(dim))

        if isinstance(chunks, dict):
            chunks = self._chunk_tuple_from_dict(chunks)
        self.chunks = chunks
        dask_manager = DaskManager(self.chunks)
        for var in self.data_vars():
            data = self.get(var)
            if data is not None:
                self.set(var, dask_manager.dask_me(data, chunks))

    @property
    def x_str(self) -> str:
        """Return string compatible with the type of spacing used:

        'x' for cartesian grid.
        'lon' for spherical grid.
        """
        if not self._structure_initialized():
            return None
        return self._x_str

    @x_str.setter
    def x_str(self, new_str):
        if new_str in ["x", "lon"]:
            self._x_str = new_str
        else:
            raise ValueError("x_str need to be 'x' or 'lon'")

    @property
    def y_str(self) -> str:
        """Return string compatible with the type of spacing used:

        'y' for cartesian grid.
        'lat' for spherical grid.
        """
        if not self._structure_initialized():
            return None
        return self._y_str

    @y_str.setter
    def y_str(self, new_str):
        if new_str in ["y", "lat"]:
            self._y_str = new_str
        else:
            raise ValueError("y_str need to be 'y' or 'lat'")

    @property
    def name(self) -> str:
        if not hasattr(self, "_name"):
            return "LonelySkeleton"
        return self._name

    @name.setter
    def name(self, new_name):
        if isinstance(new_name, str):
            self._name = new_name
        else:
            raise ValueError("name needs to be a string")

    def _chunk_tuple_from_dict(self, chunk_dict: dict) -> tuple[int]:
        """Determines a tuple of chunks based on a dict of coordinates and chunks"""
        chunk_list = []
        for coord in self.coords():
            chunk_list.append(chunk_dict.get(coord, "auto"))
        return tuple(chunk_list)

    def _structure_initialized(self) -> bool:
        return hasattr(self, "_ds_manager")

    def _skeleton_empty(self) -> bool:
        if not self._structure_initialized():
            return True
        return len(self.ds()[self.x_str]) == 0 and len(self.ds()[self.y_str]) == 0

    def _reset_masks(self) -> None:
        """Resets the mask to default values."""
        for name in self._coord_manager.added_masks():
            # update-method sets empty mask when no value is provided
            self.set(name)

    def _reset_datavars(self) -> None:
        """Resets the data variables to default values."""
        for name in self._coord_manager.added_vars():
            # update-method sets empty data when no value is provided
            self.set(name)

    def iterate(self, coords: list[str] = None):
        coords = coords or self.coords("grid")
        return iter(self)(coords)

    def __iter__(self):

        return SkeletonIterator(
            self.coords_dict("all"),
            self.coords("grid"),
            self,
        )

    def __repr__(self) -> str:
        def add_coords(list_of_coords, string, empty_list_string="") -> str:
            if not list_of_coords:
                return string + empty_list_string
            string += "("
            for c in list_of_coords:
                string += f"{c}, "
            string = string[:-2]
            string += ")"
            return string

        string = f"<{type(self).__name__} ({self.__class__.__base__.__name__})>\n"

        string += "-" * 31 + " Coordinate groups " + "-" * 30 + "\n"
        string += f"{'Spatial:':12}"

        string = add_coords(self.coords("spatial"), string, "*empty*")
        string += f"\n{'Grid:':12}"
        string = add_coords(self.coords("grid"), string, "*empty*")
        string += f"\n{'Gridpoint:':12}"
        string = add_coords(self.coords("gridpoint"), string, "*empty*")

        string += f"\n{'All:':12}"
        string = add_coords(self.coords("all"), string, "*empty*")

        string += "\n" + "-" * 36 + " Xarray " + "-" * 36 + "\n"
        string += self.ds().__repr__()

        empty_vars = self._ds_manager.empty_vars()
        empty_masks = self._ds_manager.empty_masks()

        if empty_masks or empty_vars:
            string += "\n" + "-" * 34 + " Empty data " + "-" * 34

            if empty_vars:
                string += "\n" + "Empty variables:"
                max_len = len(max(empty_vars, key=len))
                for var in empty_vars:
                    string += f"\n    {var:{max_len+2}}"
                    string = add_coords(self.coords(self.coord_group(var)), string)
                    string += f":  {self._coord_manager._default_values.get(var)}"

                    empty_vars = self._ds_manager.empty_vars()

            if empty_masks:
                string += "\n" + "Empty masks:"
                max_len = len(max(empty_masks, key=len))
                for mask in empty_masks:
                    string += f"\n    {mask:{max_len+2}}"
                    string = add_coords(self.coords(self.coord_group(mask)), string)
                    string += (
                        f":  {bool(self._coord_manager._default_values.get(mask))}"
                    )

        magnitudes = self._coord_manager.magnitudes

        if magnitudes:
            string += "\n" + "-" * 27 + " Magnitudes and directions" + "-" * 27
            for key, value in magnitudes.items():
                string += f"\n  {key}: magnitude of ({value['x']},{value['y']})"

        directions = self._coord_manager.directions
        if directions:
            for key, value in directions.items():
                string += f"\n  {key}: direction of ({value['x']},{value['y']})"
        string += "\n" + "-" * 80

        return string


def _data_vars(self) -> None:
    """Used for instanes instead of the class method, since data_variables can be added after initialization."""
    return list(self._coord_manager.added_vars().keys())
