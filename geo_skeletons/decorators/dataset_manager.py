import numpy as np
import xarray as xr
from .coordinate_manager import CoordinateManager

from ..errors import DataWrongDimensionError, UnknownCoordinateError, CoordinateWrongLengthError, GridError


def move_time_dim_to_front(coord_list) -> list[str]:
    if "time" not in coord_list:
        return coord_list
    coord_list.insert(0, coord_list.pop(coord_list.index("time")))
    return coord_list


SPATIAL_COORDS = ["y", "x", "lat", "lon", "inds"]


class DatasetManager:
    """Contains methods related to the creation and handling of the Xarray
    Dataset that will be used in any object that inherits from Skeleton."""

    def __init__(self, coordinate_manager: CoordinateManager) -> None:
        self.coord_manager = coordinate_manager

    def create_structure(
        self, x: np.ndarray, y: np.ndarray, x_str: str, y_str: str, **kwargs
    ) -> xr.Dataset:
        """Create a Dataset containing only the relevant coordinates.

        x_str, y_str = 'x', 'y' means x, y are cartesiant coordinates
        x_str, y_str = 'lon', 'lat' means x, y are spherical coordinates

        **kwargs contains any additional coordinates (e.g. time)
        """

        def check_consistency() -> None:
            """Checks that the provided coordinates are consistent with the
            coordinates that the Skeleton is defined over."""
            ds_coords = list(coord_dict.keys())
            # Check spatial coordinates
            xy_set = "x" in ds_coords and "y" in ds_coords
            lonlat_set = "lon" in ds_coords and "lat" in ds_coords
            inds_set = "inds" in ds_coords
            if inds_set:
                ind_len = len(coord_dict["inds"])
                for key, value in var_dict.items():
                    if len(value[1]) != ind_len:
                        raise CoordinateWrongLengthError(variable=key, len_of_variable=len(value[1]), index_variable='inds', len_of_index_variable=ind_len)
            if not (xy_set or lonlat_set or inds_set):
                raise GridError
            if sum([xy_set, lonlat_set, inds_set]) > 1:
                raise GridError

            # Check that all added coordinates are provided
            for coord in self.coord_manager.added_coords("all"):
                if coord not in ds_coords:
                    if self.get(coord) is not None:
                        # Add in old coordinate if it is not provided now (can happen when using set_spacing)
                        coord_dict[coord] = self.ds().get(coord).values
                    else:
                        raise UnknownCoordinateError(f"Skeleton has coordinate '{coord}', but it was not provided on initialization: {ds_coords}!")

            # Check that all provided coordinates have been added
            for coord in set(ds_coords) - set(SPATIAL_COORDS):
                if coord not in self.coord_manager.added_coords("all"):
                    raise UnknownCoordinateError(f"Coordinate {coord} provided on initialization, but Skeleton doesn't have it: {self.coord_manager.added_coords('all')}! Missing a decorator?")

        def determine_coords() -> dict:
            """Creates dictonary of the coordinates"""

            coord_dict = {}
            if "y" in self.coord_manager.initial_coords():
                coord_dict[y_str] = y
            if "x" in self.coord_manager.initial_coords():
                coord_dict[x_str] = x
            if "inds" in self.coord_manager.initial_coords():
                coord_dict["inds"] = np.arange(len(x))

            # Add in other possible coordinates that are set at initialization
            for key in self.coord_manager.added_coords():
                value = kwargs.get(key)

                if value is None:
                    value = self.get(key)

                if value is None:
                    raise UnknownCoordinateError(f"Skeleton has coordinate '{key}', but it was not provided on initialization {kwargs.keys()} nor is it already set {self.coords()}!")
                
                coord_dict[key] = np.array(value)

            coord_dict = {
                c: coord_dict[c] for c in move_time_dim_to_front(list(coord_dict))
            }

            return coord_dict

        def determine_vars() -> dict:
            """Creates dictionary of variables"""
            var_dict = {}
            initial_vars = self.coord_manager.initial_vars()
            if "y" in initial_vars.keys():
                if initial_vars["y"] not in coord_dict.keys():
                    raise ValueError(
                        f"Trying to make variable 'y' depend on {initial_vars['y']}, but {initial_vars['y']} is not set as a coordinate!"
                    )
                var_dict[y_str] = ([initial_vars["y"]], y)
            if "x" in initial_vars.keys():
                if initial_vars["x"] not in coord_dict.keys():
                    raise ValueError(
                        f"Trying to make variable 'x' depend on {initial_vars['x']}, but {initial_vars['x']} is not set as a coordinate!"
                    )
                var_dict[x_str] = ([initial_vars["x"]], x)

            return var_dict

        indexed = "inds" in self.coord_manager.initial_coords()
        x, y = clean_coordinate_vectors(
            x, y, is_cartesian=(x_str == "x"), indexed=indexed
        )

        coord_dict = determine_coords()
        var_dict = determine_vars()

        check_consistency()
        self.set_new_ds(xr.Dataset(coords=coord_dict, data_vars=var_dict))

    def set_new_ds(self, ds: xr.Dataset) -> None:
        self.data = ds

    def ds(self):
        """Resturns the Dataset (None if doesn't exist)."""
        if not hasattr(self, "data"):
            return None
        return self.data

    def set(self, data: np.ndarray, data_name: str, coord_type: str = "all") -> None:
        """Adds in new data to the Dataset.

        coord_type = 'all', 'spatial', 'grid' or 'gridpoint'
        """

        self._merge_in_ds(self.compile_to_ds(data, data_name, coord_type))

    def get(self, name: str, default_data=None, empty: bool=False, **kwargs) -> xr.DataArray:
        """Gets data from Dataset.

        **kwargs can be used for slicing data.

        """
        ds = self.ds()
        if ds is None:
            return None

        if empty:
            coords = self.coord_manager.added_vars().get(name)
            if coords is None:
                coords = self.coord_manager.added_masks().get(name)
                
            empty_data = np.full(self.coords_to_size(self.coords(coords)), self.coord_manager.default_values.get(name))
            default_data = xr.DataArray(data=empty_data,coords=self.coords_dict(coords))

        data = ds.get(name, default_data)
        if not isinstance(data, xr.DataArray):
            return data
        
        data = self._slice_data(data, **kwargs)
        if empty:
            data.values = np.full(data.shape, self.coord_manager.default_values.get(name))

        return data

    def set_attrs(self, attributes: dict, data_array_name: str = None) -> None:
        """Sets attributes to DataArray da_name.

        If data_array_name is not given, sets global attributes
        """
        if data_array_name is None:
            self.data.attrs = attributes
        else:
            self.data.get(data_array_name).attrs = attributes

    def _slice_data(self, data, **kwargs) -> xr.DataArray:
        coordinates = {}
        keywords = {}
        for key, value in kwargs.items():
            if key in list(data.coords):
                coordinates[key] = value
            else:
                keywords[key] = value

        for key, value in coordinates.items():
            #data = eval(f"data.sel({key}={value}, **keywords)")
            data = data.sel({key: value}, **keywords)

        return data

    def _merge_in_ds(self, ds_list: list[xr.Dataset]) -> None:
        """Merge in Datasets with some data into the existing Dataset of the
        Skeleton.
        """
        if not isinstance(ds_list, list):
            ds_list = [ds_list]
        for ds in ds_list:
            self.set_new_ds(ds.merge(self.ds(), compat="override"))

    def compile_to_ds(
        self, data: np.ndarray, data_name: str, coord_type: str
    ) -> xr.Dataset:
        """This is used to compile a Dataset containing the given data using the
        coordinates of the Skeleton.

        coord_type determines over which coordinates to set the mask:

        'all': all coordinates in the Dataset
        'spatial': Dataset coordinates from the Skeleton (x, y, lon, lat, inds)
        'grid': coordinates for the grid (e.g. z, time)
        'gridpoint': coordinates for a grid point (e.g. frequency, direcion or time)
        """
        coords_dict = self.coords_dict(coord_type)

        coord_shape = tuple(len(x) for x in coords_dict.values())
        if coord_shape != data.shape:
            raise DataWrongDimensionError(data_shape = data.shape, coord_shape=coord_shape)

        vars_dict = {data_name: (coords_dict.keys(), data)}

        ds = xr.Dataset(data_vars=vars_dict, coords=coords_dict)
        return ds

    def vars(self) -> list[str]:
        """Returns a list of the variables in the Dataset."""
        if self.ds() is None:
            return []
        return list(self.data.keys())
        
    def vars_dict(self) -> list[str]:
        """Returns a dict of the variables in the Dataset."""
        return self.keys_to_dict(self.vars())

    def coords(self, coords: str = "all") -> list[str]:
        """Returns a list of the coordinates from the Dataset.

        'all' [default]: all coordinates in the Dataset
        'spatial': Dataset coordinates from the Skeleton (x, y, lon, lat, inds)
        'grid': coordinates for the grid (e.g. z, time)
        'gridpoint': coordinates for a grid point (e.g. frequency, direcion or time)
        """

        def list_intersection(list1, list2):
            """Uning intersections of sets doesn't necessarily preserve order"""
            list3 = []
            for val in list1:
                if val in list2:
                    list3.append(val)
            return list3

        if coords not in ["all", "spatial", "grid", "gridpoint"]:
            raise ValueError(
                f"Keyword 'coords' needs to be 'all' (default), 'spatial', 'grid' or 'gridpoint', not {coords}."
            )

        if self.ds() is None:
            return []

        all_coords = list(self.ds().coords)

        if coords == "all":
            return move_time_dim_to_front(all_coords)
        if coords == "spatial":
            return move_time_dim_to_front(list_intersection(all_coords, SPATIAL_COORDS))
        if coords == "grid":
            return move_time_dim_to_front(
                self.coords("spatial") + self.coord_manager.added_coords("grid")
            )
        if coords == "gridpoint":
            return move_time_dim_to_front(self.coord_manager.added_coords("gridpoint"))

    def keys_to_dict(self, coords: list[str]) -> dict:
        """Takes a list of coordinates and returns a dictionary."""
        coords_dict = {}
        for coord in coords:
            coords_dict[coord] = self.get(coord)
        return coords_dict

    def coords_dict(self, type: str = "all") -> dict:
        """Return variable dictionary of the Dataset.

        'all': all coordinates in the Dataset
        'spatial': Dataset coordinates from the Skeleton (x, y, lon, lat, inds)
        'grid': coordinates for the grid (e.g. z, time)
        'gridpoint': coordinates for a grid point (e.g. frequency, direcion or time)
        """
        return self.keys_to_dict(self.coords(type))

    def coords_to_size(self, coords: list[str], **kwargs) -> tuple[int]:
        list = []
        data = self._slice_data(self.ds(), **kwargs)
        for coord in coords:
            list.append(len(data.get(coord)))

        return tuple(list)


def clean_coordinate_vectors(x, y, is_cartesian, indexed):
    """Cleans up the coordinate vectors to make sure they are numpy arrays and
    have the right dimensions in case of single points etc.
    """

    def clean_lons(lon):
        mask = lon < -180
        lon[mask] = lon[mask] + 360
        mask = lon > 180
        lon[mask] = lon[mask] - 360
        return lon

    x = np.array(x)
    y = np.array(y)

    if not x.shape:
        x = np.array([x])

    if not y.shape:
        y = np.array([y])

    if not is_cartesian:
        # force lon to be -180, 180
        x = clean_lons(x)

    if not indexed:
        if (
            len(np.unique(x)) == 1 and len(x) == 2
        ):  # e.g. lon=(4.0, 4.0) should behave like lon=4.0
            x = np.unique(x)

        if len(np.unique(y)) == 1 and len(y) == 2:
            y = np.unique(y)

    return x, y
