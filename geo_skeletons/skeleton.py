import numpy as np
import xarray as xr
import utm as utm_module
from copy import copy
from .decorators.dataset_manager import DatasetManager
from .decorators.coordinate_manager import CoordinateManager
from typing import Iterable, Union
from .distance_functions import min_distance, min_cartesian_distance
from .errors import DataWrongDimensionError
import itertools
import pandas as pd
from typing import Iterable

DEFAULT_UTM = (33, "W")
VALID_UTM_ZONES = [
    "C",
    "D",
    "E",
    "F",
    "G",
    "H",
    "J",
    "K",
    "L",
    "M",
    "N",
    "P",
    "Q",
    "R",
    "S",
    "T",
    "U",
    "V",
    "W",
    "X",
]

VALID_UTM_NUMBERS = np.linspace(1, 60, 60).astype(int)


class SkeletonIterator:
    def __init__(
        self, dict_of_coords: dict, coords_to_iterate: list[str], skeleton
    ) -> None:
        self.coords_to_iterate = coords_to_iterate
        self.dict_of_coords = dict_of_coords
        self.skeleton = skeleton
        self._compile_list()

    def __iter__(self):
        return self

    def __next__(self):
        self.ct += 1
        if self.ct < len(self.list_of_skeletons):
            return self.list_of_skeletons[self.ct]
        raise StopIteration

    def __call__(self, coords_to_iterate: list[str]):
        self.coords_to_iterate = coords_to_iterate
        self._compile_list()
        return self

    def _compile_list(self):
        coord_dict = {}
        for coord in self.coords_to_iterate:

            coord_value = self.dict_of_coords.get(coord)

            if coord_value is None:
                print(
                    f"Cannot iterate over coord {coord}, since it does not exist: {self.dict_of_coords.keys()}"
                )
            else:
                coord_dict[coord] = coord_value

        coord_tuples = itertools.product(
            *[val.values for __, val in coord_dict.items()]
        )
        list_of_skeletons = []
        for ctuple in coord_tuples:
            arg_dict = {}
            for n, val in enumerate(ctuple):
                arg_dict[list(coord_dict.keys())[n]] = val
            list_of_skeletons.append(self.skeleton.sel(arg_dict))

        self.list_of_skeletons = list_of_skeletons
        self.ct = -1


class Skeleton:
    """Contains methods and data of the spatial x,y / lon, lat coordinates and
    makes possible conversions between them.

    Keeps track of the native structure of the grid (cartesian UTM / sperical).
    """

    def __init__(
        self, x=None, y=None, lon=None, lat=None, name: str = "LonelySkeleton", **kwargs
    ) -> None:
        self.name = name
        self._init_structure(x, y, lon, lat, **kwargs)

    @classmethod
    def from_ds(cls, ds: xr.Dataset, **kwargs):
        """Generats a PointSkeleton from an xarray Dataset. All coordinates must be present, but only matching data variables included."""
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

    def __iter__(self):
        return SkeletonIterator(
            self._ds_manager.coords_dict("all"),
            self._ds_manager.coords_dict("grid").keys(),
            self,
        )

    def _init_structure(self, x=None, y=None, lon=None, lat=None, **kwargs) -> None:
        """Determines grid type (Cartesian/Spherical), generates a DatasetManager
        and initializes the Xarray dataset within the DatasetManager.

        The initial coordinates and variables are read from the method of the
        subclass (e.g. PointSkeleton)
        """

        # Migth have been already created by decorators
        if not hasattr(self, "_coord_manager"):
            self._coord_manager = CoordinateManager()

        # Initial values defined in subclass (e.g. GriddedSkeleton)
        self._coord_manager.set_initial_coords(self._initial_coords())
        self._coord_manager.set_initial_vars(self._initial_vars())

        x, y, lon, lat, kwargs = sanitize_input(
            x, y, lon, lat, self.is_gridded(), **kwargs
        )

        x_str, y_str, xvec, yvec = will_grid_be_spherical_or_cartesian(x, y, lon, lat)

        # if not self._structure_initialized() and self.is_gridded():
        #     xvec, yvec = coord_len_to_max_two(xvec), coord_len_to_max_two(yvec)

        self.x_str = x_str
        self.y_str = y_str

        # The manager contains the Xarray Dataset
        if not self._structure_initialized():
            self._ds_manager = DatasetManager(self._coord_manager)
        self._ds_manager.create_structure(xvec, yvec, self.x_str, self.y_str, **kwargs)
        self.set_utm(silent=True)

        self._reset_masks()
        self._reset_datavars()

    def _structure_initialized(self) -> bool:
        return hasattr(self, "_ds_manager")

    def _skeleton_empty(self) -> bool:
        if not self._structure_initialized():
            return True
        return len(self.ds()[self.x_str]) == 0 and len(self.ds()[self.y_str]) == 0

    def absorb(self, skeleton_to_absorb, dim: str) -> None:
        """Absorb another object of same type. This is used e.g. when pathcing
        cached data and joining different Boundary etc. over time.
        """
        if not self.is_gridded() and dim == "inds":
            inds = skeleton_to_absorb.inds() + len(self.inds())
            skeleton_to_absorb.ds()["inds"] = inds

        new_skeleton = self.from_ds(
            xr.concat(
                [self.ds(), skeleton_to_absorb.ds()], dim=dim, data_vars="minimal"
            ).sortby(dim)
        )
        return new_skeleton

    def _reset_masks(self) -> None:
        """Resets the mask to default values."""
        for name in self._coord_manager.added_masks():
            # update-method sets empty mask when no is provided
            self.set(name)

    def _reset_datavars(self) -> None:
        """Resets the data variables to default values."""
        for name in self._coord_manager.added_vars():
            # update-method sets empty mask when no is provided
            self.set(name)

    def data_vars(self) -> None:
        return list(self._coord_manager.added_vars().keys())

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

        .insert(name='geodata', data=data_slice, time=0, threshold=0)"""

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
        allow_reshape: bool = False,
        coords: list[str] = None,
        silent: bool = True,
    ) -> None:
        var_coord_type = self._coord_manager.added_vars().get(name)
        mask_coord_type = self._coord_manager.added_masks().get(name)

        coord_type = var_coord_type or mask_coord_type
        metadata = self.metadata()
        if coord_type is None:
            raise ValueError(
                f"A data variable named {name} has not been defines ({list(self._coord_manager.added_vars().keys())}, {list(self._coord_manager.added_masks().keys())})"
            )

        if data is None:
            data = self.get(name, empty=True)

        if mask_coord_type is not None:
            data = data.astype(int)

        if isinstance(data, xr.DataArray):
            coords = list(data.dims)
            data = data.values

        if coords is not None:
            data_coordinates = list(self.get(name, data_array=True).dims)
            if len(data_coordinates) != len(coords):
                allow_reshape = (
                    True  # We have some trivial dimension that needs to be expanded
                )
            shape_list = [coords.index(c) for c in data_coordinates if c in coords]
            if shape_list != [
                n for n in range(len(shape_list))
            ]:  # Don't reshape trivially
                if not silent:
                    print(
                        f"Reshaping data {data.shape} -> {np.transpose(data, tuple(shape_list)).shape}: {coords} -> {data_coordinates}"
                    )
                data = np.transpose(data, tuple(shape_list))

        try:
            self._ds_manager.set(data=data, data_name=name, coord_type=coord_type)
        except DataWrongDimensionError as data_error:
            if allow_reshape:
                if not silent:
                    print(
                        f"Size of {name} does not match size of {type(self).__name__}, trying to reshape..."
                    )
                if (
                    len(data.shape) == 2
                    and len(self.size(coord_type)) == 2
                    and sum(data.shape) == sum(self.size(coord_type))
                ):
                    if not silent:
                        print(f"Transposing data {data.shape} -> {data.T.shape}...")
                    reshaped_data = data.T
                elif (
                    len(data.shape) == 2
                    and len(self.size(coord_type, squeeze=True)) == 2
                    and sum(data.shape) == sum(self.size(coord_type, squeeze=True))
                    and data.shape != self.size(coord_type, squeeze=True)
                ):
                    if not silent:
                        print(
                            f"Transposing and reshaping data {data.shape} -> {data.T.shape} -> {self.size(coord_type)}"
                        )
                    reshaped_data = np.reshape(data.T, self.size(coord_type))
                else:
                    if len(data.shape) > len(self.size(coord_type)):
                        if not silent:
                            print(
                                f"Squeezing data {data.shape} -> {self.size(coord_type)}..."
                            )
                    else:
                        if not silent:
                            print(
                                f"Expanding data {data.shape} -> {self.size(coord_type)}..."
                            )
                    reshaped_data = np.reshape(data, self.size(coord_type))
                self._ds_manager.set(
                    data=reshaped_data, data_name=name, coord_type=coord_type
                )
            else:
                raise data_error

        self.set_metadata(metadata)

    def get(
        self,
        name,
        empty=False,
        data_array: bool = False,
        squeeze: bool = False,
        boolean_mask: bool = False,
        **kwargs,
    ):
        """Gets a mask or data variable.

        The ds_manager always gets what is in the Dataset (integers for masks).
        The Skeletons get-method gives boolen masks, and you can also
        request empty masks that will be return even if data doesn't exist."""
        if not self._structure_initialized():
            return None

        data = self._ds_manager.get(name, empty=empty, **kwargs)

        if not isinstance(data, xr.DataArray):
            return None

        data = data.copy()

        if boolean_mask:
            data.values = data.values.astype(bool)

        if squeeze:
            data = data.squeeze(drop=True)

        if data_array:
            return data
        else:
            return data.values

    def is_empty(self, name):
        """Checks if a Dataset variable is empty.

        Empty means all initial values OR all 0 values."""
        data = self.get(name)
        if data is None:
            return False
        empty_data = self.get(name, empty=True)
        land_data = data * 0
        is_empty = np.allclose(
            data.astype(float), empty_data.astype(float)
        ) or np.allclose(data.astype(float), land_data.astype(float))
        return is_empty

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

        if not valid_utm_zone((number, letter)):
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
        'grid': size over coordinates for the grid (e.g. z, time)
        'gridpoint': size over coordinates for a grid point (e.g. frequency, direcion or time)
        """

        if not self._structure_initialized():
            return None

        size = self._ds_manager.coords_to_size(
            self._ds_manager.coords(coords), **kwargs
        )

        if squeeze:
            size = tuple([s for s in size if s > 1])
        return size

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

        if not self.is_cartesian() and (strict or self.strict):
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
            lat = cap_lat_for_utm(lat)

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

        if not self.is_cartesian() and (strict or self.strict):
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
            lat = cap_lat_for_utm(self.lat(**kwargs))
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

        if self.is_cartesian() and (strict or self.strict):
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

        if self.is_cartesian() and (strict or self.strict):
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

        if not self.is_cartesian() and (strict or self.strict) and (not native):
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

        if not self.is_cartesian() and (strict or self.strict) and (not native):
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

        if self.is_cartesian() and (strict or self.strict) and (not native):
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

        if self.is_cartesian() and (strict or self.strict) and (not native):
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
        x = force_to_iterable(x)
        y = force_to_iterable(y)
        lon = force_to_iterable(lon)
        lat = force_to_iterable(lat)

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
                dxx, ii = min_distance(
                    llon, llat, lonlist, latlist
                )  # Slower, but works for high/low latitudes and is exact
            else:
                dxx, ii = min_cartesian_distance(xx, yy, xlist, ylist)
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
    def strict(self) -> bool:
        """If this is set to true, then no coordinate conversion will ever be
        made when requesting lon, lat, x, y etc."""
        if not hasattr(self, "_strict"):
            return False
        return self._strict

    @strict.setter
    def strict(self, strict: bool) -> None:
        if isinstance(strict, bool):
            self._strict = strict
        else:
            raise ValueError("strict needs to be a bool")

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


def coord_len_to_max_two(xvec):
    if xvec is not None and len(xvec) > 2:
        xvec = np.array([min(xvec), max(xvec)])
    return xvec


def sanitize_singe_variable(name: str, x):
    """Forces to nump array and checks dimensions etc"""
    x = force_to_iterable(x)

    # np.array([None, None]) -> None
    if x is None or all(v is None for v in x):
        x = None

    if x is not None and len(x.shape) > 1:
        raise Exception(
            f"Vector {name} should have one dimension, but it has dimensions {x.shape}!"
        )

    # Set np.array([]) to None
    if x is not None and x.shape == (0,):
        x = None

    return x


def sanitize_point_structure(spatial: dict) -> dict:
    """Repeats a single value to match lenths of arrays"""
    x = spatial.get("x")
    y = spatial.get("y")
    lon = spatial.get("lon")
    lat = spatial.get("lat")

    if x is not None and y is not None:
        if len(x) != len(y):
            if len(x) == 1:
                spatial["x"] = np.repeat(x[0], len(y))
            elif len(y) == 1:
                spatial["y"] = np.repeat(y[0], len(x))
            else:
                raise Exception(
                    f"x-vector is {len(x)} long but y-vecor is {len(y)} long!"
                )
    if lon is not None and lat is not None:
        if len(lon) != len(lat):
            if len(lon) == 1:
                spatial["lon"] = np.repeat(lon[0], len(lat))
            elif len(lat) == 1:
                spatial["lat"] = np.repeat(lat[0], len(lon))
            else:
                raise Exception(
                    f"x-vector is {len(lon)} long but y-vecor is {len(lat)} long!"
                )

    return spatial


def get_edges_of_arrays(spatial: dict) -> dict:
    """Takes only edges of arrays, so [1,2,3] -> [1,3]"""
    for key, value in spatial.items():
        if value is not None:
            spatial[key] = coord_len_to_max_two(value)

    return spatial


def check_that_variables_equal_length(x, y) -> bool:
    if x is None and y is None:
        return True
    if x is None:
        raise ValueError(f"x/lon variable None even though y/lat variable is not!")
    if y is None:
        raise ValueError(f"y/lat variable None even though x/lon variable is not!")
    return len(x) == len(y)


def sanitize_time_input(time):
    if isinstance(time, str):
        return pd.DatetimeIndex([time])
    if not isinstance(time, Iterable):
        return pd.DatetimeIndex([time])
    return pd.DatetimeIndex(time)


def sanitize_input(x, y, lon, lat, is_gridded_format, **kwargs):
    """Sanitizes input. After this all variables are either
    non-empty np.ndarrays with len >= 1 or None"""

    spatial = {"x": x, "y": y, "lon": lon, "lat": lat}
    for key, value in spatial.items():
        spatial[key] = sanitize_singe_variable(key, value)

    other = {}
    for key, value in kwargs.items():
        if key == "time":
            # other[key] = sanitize_singe_variable(key, value, fmt="datetime")
            other[key] = sanitize_time_input(value)
        else:
            other[key] = sanitize_singe_variable(key, value)

    if not is_gridded_format:
        spatial = sanitize_point_structure(spatial)

        for x, y in [("x", "y"), ("lon", "lat")]:
            length_ok = check_that_variables_equal_length(spatial[x], spatial[y])
            if not length_ok:
                raise Exception(
                    f"{x} is length {len(spatial[x])} but {y} is length {len(spatial[y])}!"
                )

    if np.all([a is None for a in spatial.values()]):
        raise Exception("x, y, lon, lat cannot ALL be None!")

    return spatial["x"], spatial["y"], spatial["lon"], spatial["lat"], other


def will_grid_be_spherical_or_cartesian(x, y, lon, lat):
    """Determines if the grid will be spherical or cartesian based on which
    inputs are given and which are None.

    Returns the ringth vector and string to identify the native values.
    """

    # Check for empty grid
    if (
        (lon is None or len(lon) == 0)
        and (lat is None or len(lat) == 0)
        and (x is None or len(x) == 0)
        and (y is None or len(y) == 0)
    ):
        native_x = "x"
        native_y = "y"
        xvec = np.array([])
        yvec = np.array([])
        return native_x, native_y, xvec, yvec

    xy = False
    lonlat = False

    if (x is not None) and (y is not None):
        xy = True
        native_x = "x"
        native_y = "y"
        xvec = x
        yvec = y

    if (lon is not None) and (lat is not None):
        lonlat = True
        native_x = "lon"
        native_y = "lat"
        xvec = lon
        yvec = lat

    if xy and lonlat:
        raise ValueError("Can't set both lon/lat and x/y!")

    # Empty grid will be cartesian
    if not xy and not lonlat:
        native_x = "x"
        native_y = "y"
        xvec = np.array([])
        yvec = np.array([])

    return native_x, native_y, xvec, yvec


def cap_lat_for_utm(lat):
    if isinstance(lat, float):
        lat = np.array([lat])
    if len(lat) > 0 and max(lat) > 84:
        print(
            f"Max latitude {max(lat)}>84. These points well be capped to 84 deg in UTM conversion!"
        )
        lat[lat > 84.0] = 84.0
    if len(lat) > 0 and min(lat) < -80:
        lat[lat < -80.0] = -80.0
        print(
            f"Min latitude {min(lat)}<-80. These points well be capped to -80 deg in UTM conversion!"
        )
    return lat


def force_to_iterable(x, fmt: str = None) -> Iterable:
    """Returns an numpy array with at least one dimension and Nones removed

    Will return None if given None."""
    if x is None:
        return None

    x = np.atleast_1d(x)
    x = np.array([a for a in x if a is not None])

    return x


def valid_utm_zone(utm_zone: tuple[int, str]) -> bool:
    """Checks that a UTM zone, e.g. (33, 'V') is valid."""

    zone_number, zone_letter = utm_zone

    if zone_number not in VALID_UTM_NUMBERS:
        return False

    if zone_letter not in VALID_UTM_ZONES:
        return False

    return True
