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
    remap_coords_of_ds_vars_to_skeleton_names,
    gather_coord_values,
    find_proj
)
from . import data_sanitizer as sanitize
from .managers.proj_manager import ProjManager
from typing import Iterable, Union, Optional
from . import distance_funcs
from .errors import (
    DataWrongDimensionError,
    DirTypeError,
    SkeletonError,
    UnknownVariableError,
    MissingDatasetError
)
from collections.abc import Iterable
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
from geo_skeletons.errors import ProjectionError
from geo_skeletons import dask_computations, dir_conversions
import itertools
import dask.array as da
import geo_parameters as gp
from geo_parameters.metaparameter import MetaParameter
from geo_parameters.model_aliases import WAM
from .distance_funcs import distance_2points
import pandas as pd
from copy import deepcopy


CoordinateValue = Union[Iterable[float], Iterable[int], float, int]
CRSValue = Union[int, str, tuple[int, str], dict]
class Skeleton:
    """Contains methods and data of the spatial x,y / lon, lat coordinates and
    makes possible conversions between them.

    Keeps track of the native structure of the grid (cartesian UTM / sperical).
    """

    chunks = None

    def __init__(
        self,
        x: Optional[CoordinateValue] = None,
        y: Optional[CoordinateValue] = None,
        lon: Optional[CoordinateValue] = None,
        lat: Optional[CoordinateValue] = None,
        name: str = "LonelySkeleton",
        crs: Optional[CRSValue] = None,
        chunks: Optional[Union[tuple[int], str]] = None,
        **kwargs,
    ) -> None:
        self._init_structure(x, y, lon, lat, **kwargs)
        self._init_managers(crs=crs, chunks=chunks)
        self._init_metadata(name=name)

    def _init_structure(self, x: Optional[CoordinateValue] = None, y: Optional[CoordinateValue] = None, lon: Optional[CoordinateValue] = None, lat: Optional[CoordinateValue] = None, **kwargs) -> None:
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
        self.core._set_initial_coords(self._initial_coords(spherical=(x_str == "lon")))
        self.core._set_initial_vars(self._initial_vars(spherical=(x_str == "lon")))

        self._ds_manager.create_structure(x=xvec, y=yvec, new_coords=kwargs)

    def _init_managers(self, crs: CRSValue, chunks: Union[tuple[int], str]) -> None:
        """Initialized a DirTypeManager, UTMManager and DaskManager, and sets a UTM-zone"""
        if chunks is None:
            if hasattr(self, "_chunks"):  # Set by @activate_dask-decorator
                chunks = self._chunks

        self.dask = DaskManager(skeleton=self, chunks=chunks)
        self.proj = ProjManager(crs=crs, lon=self.edges("lon", strict=True),lat=self.edges("lat", strict=True), x=self.edges("x", strict=True),y=self.edges("y", strict=True),metadata_manager=self.meta)
        self.core.proj = self.proj
        if crs is None and not self.core.is_projected():
            self.proj.reset_utm(silent=True)
        self.resample = ResampleManager(self)
        

    def _init_metadata(self, name: str) -> None:
        """Initialized the metadata by using availabe metadata in the GeoParameters"""
        for coord_name in self.core.all_objects("all"):
            metavar = self.core.get(coord_name).meta
            if metavar is not None:
                self.meta.append(metavar.meta_dict(), coord_name)
                if metavar.i_am() in {'x','y','direction'} or (metavar.dir_type() is not None and coord_name not in self.core.coords()):
                    self.meta.append({'rotated_according_to': 'wgs84'}, coord_name)

            if self.core.get(coord_name).coord_group in ['all', 'spatial', 'grid'] and coord_name not in ['inds', 'time']:
                if self.core.is_projected():
                    self.meta.append({'grid_mapping': 'crs'}, coord_name)
                else:
                    self.meta.append({'grid_mapping': 'wgs84'}, coord_name)

        self.meta.append({"name":name})
        self.meta.set({'epsg': 4326}, 'wgs84')

    @classmethod
    def add_time(cls, grid_coord: bool = True) -> "Skeleton":
        """Creates a new class with a time variable added.

        Args:
            grid_coord (bool, optional): Whether to add the time variable as a grid coordinate. Defaults to True.

        Returns:
            Skeleton: A new class with the time variable added.

        Examples:
            Equivalent to using:

            >>> from geo_skeletons.decorators import add_time
            >>> @add_time()
            >>> class NewClass(OldClass):
            >>>     pass
        """
        new_cls = type(_modified_name(cls.__name__), (cls,), {})
        return add_time(grid_coord=grid_coord)(new_cls)

    @classmethod
    def add_frequency(
        cls, name: Union[str, MetaParameter] = gp.wave.Freq, grid_coord: bool = False
    ) -> "Skeleton":
        """Creates a new class with a frequency variable added.

        Args:
            name (Union[str, MetaParameter], optional): Name of the frequency variable. Defaults to `gp.wave.Freq`.
            grid_coord (bool, optional): Whether to add the frequency variable as a grid coordinate. Defaults to False.

        Returns:
            Skeleton: A new class with the frequency variable added.

        Examples:
            Equivalent to using:

            >>> from geo_skeletons.decorators import add_frequency
            >>> @add_frequency()
            >>> class NewClass(OldClass):
            >>>     pass
        """
        new_cls = type(_modified_name(cls.__name__), (cls,), {})
        return add_frequency(name=name, grid_coord=grid_coord)(new_cls)

    @classmethod
    def add_direction(
        cls,
        name: Union[str, MetaParameter] = gp.wave.Dirs,
        grid_coord: bool = False,
        dir_type: Optional[bool] = None,
    ) -> "Skeleton":
        """Creates a new class with a direction variable added.

        Args:
            name (Union[str, MetaParameter], optional): Name of the direction variable. Defaults to `gp.wave.Dirs`.
            grid_coord (bool, optional): Whether to add the direction variable as a grid coordinate. Defaults to False.
            dir_type (Optional[bool], optional): Type of the direction variable, if applicable. Defaults to None.

        Returns:
            Skeleton: A new class with the direction variable added.

        Examples:
            Equivalent to using:

            >>> from geo_skeletons.decorators import add_direction
            >>> @add_direction()
            >>> class NewClass(OldClass):
            >>>     pass
        """
        new_cls = type(_modified_name(cls.__name__), (cls,), {})
        return add_direction(name=name, grid_coord=grid_coord, dir_type=dir_type)(
            new_cls
        )

    @classmethod
    def add_coord(
        cls,
        name: Union[str, MetaParameter] = "dummy",
        grid_coord: bool = False,
    ) -> "Skeleton":
        """Creates a new class with a coordinate added.

        Args:
            name (Union[str, MetaParameter], optional): Name of the coordinate to add. Defaults to "dummy".
            grid_coord (bool, optional): Whether to add the coordinate as a grid coordinate. Defaults to False.

        Returns:
            Skeleton: A new class with the coordinate added.

        Examples:
            Equivalent to using:

            >>> from geo_skeletons.decorators import add_coord
            >>> @add_coord('new_coord')
            >>> class NewClass(OldClass):
            >>>     pass
        """
        new_cls = type(_modified_name(cls.__name__), (cls,), {})
        return add_coord(name=name, grid_coord=grid_coord)(new_cls)

    @classmethod
    def add_datavar(
        cls,
        name: Union[Union[str, MetaParameter], list[Union[str, MetaParameter]]],
        coord_group: Union[str, list[str]] = "all",
        default_value: Union[float, list[float]] = 0.0,
    ) -> "Skeleton":
        """Creates a new class with a data variable added.

        Args:
            name (Union[Union[str, MetaParameter], list[Union[str, MetaParameter]]]): Name of the data variable.
            coord_group (Union[str, list[str]], optional): Coordinate group for the variable. Must be one of 'all', 'spatial', 'grid', or 'gridpoint'. Defaults to "all".
            default_value (Union[float, list[float]], optional): Default value for the data variable. Defaults to 0.0.

        Returns:
            Skeleton: A new class with the data variable added.

        Examples:
            Equivalent to using:

            >>> from geo_skeletons.decorators import add_datavar
            >>> @add_datavar('new_var')
            >>> class NewClass(OldClass):
            >>>     pass
        """
        new_cls = type(_modified_name(cls.__name__), (cls,), {})
        if not isinstance(name, list):
            name = [name]

        if not isinstance(coord_group, list):
            coord_group = [coord_group] * len(name)
        if not isinstance(default_value, list):
            default_value = [default_value] * len(name)
        
        for na, cg, dv in zip(name, coord_group, default_value):
            new_cls = add_datavar(
                name=na, coord_group=cg, default_value=dv
            )(new_cls)
        
        return new_cls
    
    @classmethod
    def add_magnitude(
        cls,
        name: Union[str, MetaParameter],
        x: str,
        y: str,
        direction: Optional[Union[str, MetaParameter]] = None,
        dir_type: Optional[str] = None,
    ) -> "Skeleton":
        """Creates a new class with a data variable for magnitude added.

        Args:
            name (Union[str, MetaParameter]): Name of the magnitude.
            x (str): Name of an already set variable to be used as the x-component.
            y (str): Name of an already set variable to be used as the y-component.
            direction (Optional[Union[str, MetaParameter]], optional): 
                Name of the direction associated with the magnitude. Defaults to None.
            dir_type (Optional[str], optional): Type of the directional parameter. 
                Must be one of 'from', 'to', or 'math'. Automatically parsed 
                if `name` is a `MetaParameter`. Defaults to None.

        Returns:
            Skeleton: A new class with the specified magnitude and direction added.

        Examples:
            If `OldClass` has data variables 'u' and 'v', the following:

            >>> NewClass = OldClass.add_magnitude(
            >>>     name='mag', x='u', y='v', direction='dir', dir_type='from'
            >>> )

            Is equivalent to using:

            >>> from geo_skeletons.decorators import add_magnitude
            >>> @add_magnitude(name='mag', x='u', y='v', direction='dir', dir_type='from')
            >>> class NewClass(OldClass):
            >>>     pass
        """

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
    ) -> "Skeleton":
        """Creates a new class with a mask added.

        Args:
            name (Union[str, MetaParameter]): Name of the mask.
            default_value (int, optional): Default value for the mask. 
                Can be 0 or 1 (False or True). Defaults to 0.
            coord_group (str, optional): Coordinate group for the mask. 
                Must be one of 'all', 'spatial', 'grid', or 'gridpoint'. 
                Defaults to 'grid'.
            opposite_name (Optional[Union[str, MetaParameter]], optional): 
                The name of the opposite mask, if applicable (e.g., 'land' 
                when `name` is 'sea'). Defaults to None.
            triggered_by (Optional[str], optional): Name of the variable 
                that triggers the mask (e.g., 'land_mask' might be triggered 
                by setting the variable 'hs'). Defaults to None.
            valid_range (tuple[float], optional): Range of values for the 
                triggered variable that define the mask. Use `None` to 
                indicate infinity. Defaults to (0.0, None).
            range_inclusive (bool, optional): If `True`, the `valid_range` 
                includes boundary values (e.g., `(0.0, None)` includes all 
                non-negative values). Defaults to True.

        Returns:
            Skeleton: A new class with the specified mask added.

        Examples:
            Equivalent to using:

            >>> from geo_skeletons.decorators import add_mask
            >>> @add_mask(name='land')
            >>> class NewClass(OldClass):
            >>>     pass
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
    def from_coord_dict(cls, coord_dict: dict) -> "Skeleton":
        """Creates an empty version of the class from a dictionary containing the coordinates"""
        return cls(**coord_dict)

    @classmethod
    def from_netcdf(
        cls, 
        filename: str,         
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
        name: Optional[str] = None, **kwargs) -> "Skeleton":
        """Generates an instance of the Skeleton class from a NetCDF file.

        This method reads a NetCDF file into an xarray Dataset and uses the `from_ds` method 
        to create the Skeleton instance. The `name` attribute for the Skeleton is set based 
        on the following priority:
        1. The `name` argument passed to this method.
        2. The `name` attribute of the xarray Dataset (if available).
        3. A default name indicating the file it was created from (`"created_from_<filename>"`).

        Args:
            filename (str): Path to the NetCDF file to read.
            chunks (Optional[Union[tuple[int], str]], optional): Chunk size for the Dataset. Can be a tuple of integers or a string. Defaults to None.
            only_vars (Optional[list[str]], optional): List of variable names in the NetCDF file to include. If `None`, all variables are read. Defaults to None.
            ignore_vars (Optional[list[str]], optional): List of variable names in the NetCDF file to exclude. Defaults to None.
            keep_ds_names (bool, optional): If `True`, uses the NetCDF variable names instead of the Skeleton's default short names. Defaults to False.
            decode_cf (bool, optional): Whether to allow decoding of CF (Climate and Forecast) standard names. Defaults to True.
            core_aliases (dict[Union[MetaParameter, str], str], optional): A dictionary mapping existing Skeleton variables to variables in the NetCDF file. Defaults to None.
            ds_aliases (dict[str, Union[MetaParameter, str]], optional): A dictionary describing NetCDF variables in terms of geo-parameters or strings. Defaults to None.
            dynamic (bool, optional): If `True`, allows the creation of new data variables. Otherwise, limits operations to existing variables. Defaults to False.
            verbose (bool, optional): If `True`, provides detailed output during the process. Defaults to False.
            meta_dict (dict, optional): Metadata dictionary to provide additional information. Defaults to None.
            name (Optional[str], optional): Name to give to the Skeleton instance. If not provided, the name is determined from the Dataset or the filename. Defaults to None.
            **kwargs: Additional keyword arguments, such as missing coordinates, passed to the `from_ds` method.

        Returns:
            Skeleton: A new Skeleton instance generated from the NetCDF file.

        Notes:
            This method is a wrapper around the `from_ds` method. For detailed information on 
            the parameters and functionality, refer to the documentation for `from_ds`.

        Examples:
            Basic usage:
            >>> new_instance = SkeletonClass.from_netcdf("example.nc")

            Specifying additional options:
            >>> new_instance = SkeletonClass.from_netcdf(
            >>>     "example.nc",
            >>>     only_vars=["temperature", "salinity"],
            >>>     core_aliases={"temp": "temperature"},
            >>>     name="Ocean Data",
            >>> )
        """
        ds = xr.open_dataset(filename)
        if hasattr(ds, 'name'):
            ds_name = ds.name
        else:
            ds_name = None
        name = name or ds_name or f"created_from_{filename}"

        return cls.from_ds(
            ds=ds,
            chunks=chunks,
            only_vars=only_vars,
            ignore_vars=ignore_vars,
            keep_ds_names=keep_ds_names,
            decode_cf=decode_cf,
            core_aliases=core_aliases,
            ds_aliases=ds_aliases,
            dynamic=dynamic,
            verbose=verbose,
            meta_dict=meta_dict,
            name=name,
            **kwargs
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
        name: Optional[str] = None,
        **kwargs,
    ) -> "Skeleton":
        """Generates an instance of a Skeleton from an xarray Dataset.

        Args:
            ds (xr.Dataset): The xarray Dataset from which the Skeleton instance is created.
            chunks (Optional[Union[tuple[int], str]], optional): Chunk size for the Dataset. Can be a tuple of integers or a string.
            only_vars (Optional[list[str]], optional): List of variable names in the Dataset to read. If `None`, all variables are read. Defaults to None.
            ignore_vars (Optional[list[str]], optional): List of variable names in the Dataset to ignore. Defaults to None.
            keep_ds_names (bool, optional): If `True`, uses the Dataset variable names instead of the Skeleton's default short names. Defaults to False.
            decode_cf (bool, optional): Whether to allow decoding of CF standard names. Defaults to True.
            core_aliases (dict[Union[MetaParameter, str], str], optional): A dictionary mapping existing Skeleton variables to variables in the Dataset. Defaults to None.
            ds_aliases (dict[str, Union[MetaParameter, str]], optional): A dictionary describing Dataset variables in terms of geo-parameters or strings. Defaults to None.
            dynamic (bool, optional): If `True`, allows the creation of new data variables. Otherwise, limits operations to existing variables. Defaults to False.
            verbose (bool, optional): If `True`, provides detailed output during the process. Defaults to False.
            meta_dict (dict, optional): Dictionary to set metadata to the Skeleton. Defaults to None.
            name (Optional[str], optional): Name to give to the Skeleton instance. Defaults to None.
            **kwargs: Additional keyword arguments, such as missing coordinates.

        Returns:
            Skeleton: A new Skeleton instance generated from the provided Dataset.

        Notes:
            **only_vars and ignore_vars**:
            - `only_vars` specifies the variables to include from the Dataset. Defaults to `[]` (all variables are read).
            - `ignore_vars` specifies the variables to exclude from the Dataset. Defaults to `[]`.

            **keep_ds_names**:
            - This is used in combination with `dynamic = True` to create variables.
            - If `True`, the variable names in the Dataset are preserved in the Skeleton.
            - If `False`, the Skeleton uses default short names for the variables.

            **core_aliases**:
            Maps existing Skeleton variables to Dataset variables. Example mappings:
            - `{'hs': 'Hm0'}`: Reads the Dataset variable 'Hm0' and maps it to the Skeleton variable 'hs'.
            - `{gp.wave.Hs: 'Hm0'}`: Reads the Dataset variable 'Hm0' and maps it to the geo-parameter `gp.wave.Hs` in the Skeleton.

            **ds_aliases**:
            Describes Dataset variables in terms of geo-parameters or strings. 
            Generally preferrable to using `core_aliases`, especially with geo-parameters, since they add a generic descrition of the xr.Dataset that can be used with many classe.
            Example mappings:
            - `{'Hm0': 'hs'}`: Reads the Dataset variable 'Hm0' and sets it as the Skeleton variable 'hs'.
            - `{'Hm0': gp.wave.Hs}`: Reads the Dataset variable 'Hm0' and creates a Skeleton variable using metadata from `gp.wave.Hs`.
            - `{'Hm0': gp.wave.Hs('hsig')}`: Reads the Dataset variable 'Hm0' and creates (if dynamic) a Skeleton variable 'hsig' using metadata from `gp.wave.Hs`.

            **Known Relationships in geo-parameters**:
            This method uses internal relationships defined in the geo-parameters module. For example:
            - `gp.wave.Tp` can be read using `gp.wave.Fp` in the Dataset (known inverse relationship).
            - `gp.wind.WindDir` can be read using `gp.wind.WindDirTo` (known opposite direction).
            - `gp.wind.WindDir` can also be read using `gp.wind.XWind` and `gp.wind.YWind` (known components).

            **Known Aliases**:
            The method uses an internal set of known aliases for parameters. However, relying on these mappings is not recommended, as they are not exhaustive. It's better to use the `ds_aliases` argument to explicitly define mappings for each variable. Examples of known aliases:
            - `'lon' <-> 'longitude' <-> gp.grid.Lon`
            - `'hs' <-> 'swh' <-> 'hm0' <-> 'hsig' <-> 'h13' <-> 'vhm0' <-> gp.wave.Hs`
            - `'xwnd' <-> gp.wind.XWind`
            - `'Pdir'` is not mapped due to ambiguity (to/from direction).
            - `'Tm'` is not mapped due to ambiguity between `gp.wave.Tm01` and `gp.wave.Tm_10`.

            **Providing Missing Coordinates**:
            Missing coordinates can be provided using `**kwargs`. For example:
            - If the 'z' coordinate is missing from the Dataset:
            >>> new_instance = SkeletonClass.from_ds(ds, z=[1, 2, 3])

        Examples:
            Basic usage:
            >>> new_instance = SkeletonClass.from_ds(ds)

            Using `core_aliases` and `ds_aliases`:
            >>> new_instance = SkeletonClass.from_ds(
            >>>     ds,
            >>>     core_aliases={'hs': 'Hm0'},
            >>>     ds_aliases={'Hm0': gp.wave.Hs}
            >>> )
    """

        meta_dict = meta_dict or {}
        core_aliases = core_aliases or {}
        ds_aliases = ds_aliases or {}
        if isinstance(ds_aliases, str):
            if ds_aliases.lower() == 'wam':
                ds_aliases = WAM 
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

        resubmit = False
        if cls.is_gridded():
            for key in ['lat', 'lon', 'x','y']:
                val = coords.get(key)
                val = np.atleast_1d(val)
                if val is not None and len(val) > 1 and val[0]> val[-1]:
                    print(f'Variable {core_coords_to_ds_coords.get(key)} is not monotonically increasing. Flipping!')
                    ds = ds.isel(**{core_coords_to_ds_coords.get(key):slice(None, None, -1)})
                    resubmit = True

            if resubmit:
                return cls.from_ds(ds=ds, chunks=chunks, only_vars = only_vars, ignore_vars = ignore_vars, 
                                    keep_ds_names= keep_ds_names,
                                    decode_cf = decode_cf, 
                                    core_aliases=core_aliases,
                                    ds_aliases = ds_aliases,
                                    dynamic = dynamic,
                                    verbose=verbose,
                                    meta_dict=meta_dict,
                                    name=name, 
                                    **kwargs)

            

            
        name = name or ds.attrs.get("name")
        points = cls(**coords, chunks=chunks, name=name)
        
        # Lengths needed for matching coordinates with wrong name
        # We do this instead of reading the lengths of the arrays directyl
        # Reason is that we want 'inds' for PointSkeletons etc
        core_lens = {c: len(points.get(c)) for c in points.core.coords("all")}

        # We might have some trivial 'x': 'x' mapping even though longitude is set, so remove note needed mappings
        core_coords_to_ds_coords = {
            c: v for (c, v) in core_coords_to_ds_coords.items() if c in coords_needed
        }
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

        metadata = {key: value for key, value in metadata.items() if key != 'name'}
        points.meta.append(metadata)

        proj_obj = find_proj(ds)
        if proj_obj:
            points.proj.set(proj_obj, silent=not verbose)
        elif points.core.is_projected():
            print('Could not decode any projection for the cartesian data!')

        return points

    def _determine_quicklook_variables(self, mag: bool, dir: bool, arrows: bool, arrow_vars: dict[str, str]) -> dict[str, dict]:
        """Determines which variables to plot and how to plot them (arrows etc)
        
        If mag = dir = arrows = False:
        Plot only normal data variables (components in case of e.g. wind)

        If mag = True:
        Plot only magnitudes
            If arrows = True:
                Plot quiver plot of direction on top of magnitude
        If dir = True:
        Plot directions as magnitudes with circular colormap

        If arrows = True:
        Plot quiver plots of directions
        """
        vars: dict[str, dict] = {}
        if dir and arrows:
            mag = True
        if mag or dir or arrows:
            for var in self.core.magnitudes(): # Every direction is connected to a magnitude
                if self.get(var, strict=True) is not None:
                    if mag:
                        vars[var] = {}
                    dirparam = self.core.get(var).direction
                    if dirparam is not None:
                        if dir:
                            vars[dirparam.name] = {'cmap': 'twilight'}
                        if arrows and mag:
                            vars[var] = {'arrow_data': dirparam.name}
                        elif arrows:
                            vars[dirparam.name] = {'is_arrow': True}
            return vars
    
        for var in self.core.data_vars():
            if self.get(var, strict=True) is not None:
                vars[var] = {}
                if self.core.get(var).dir_type is not None:
                    vars[var] = {'cmap': 'twilight'}
                if arrow_vars.get(var) is not None:
                    vars[var] = {'arrow_data': arrow_vars.get(var)}
        
       
        return vars
    
    def quicklook(self, proj: str = None, compare: "Skeleton" = None, contour: bool = True, mag: bool=False, dir: bool=False, arrows: bool=False, arrow_vars: dict[str, str] = None, rotated: Optional[bool]=None, sparse: bool=True, show: bool=True, coastline: bool=False) -> None:
        """Generates a quick visualization of the data in the Skeleton instance.

        If a time variable is present, the first time instance is displayed. This method 
        provides options for plotting projections, magnitudes, directions, arrows, and 
        comparisons with another Skeleton instance.

        Args:
            proj (str, optional): Projection for the plot. Can be one of:
                - `'lonlat'`: Longitude-latitude projection.
                - `'xy'`: Cartesian or rotated projection.
                If `None`, uses the default projection of the data. Defaults to None.
            compare ("Skeleton", optional): Another Skeleton instance to compare with. 
                The points of the comparison Skeleton are plotted in the same projection 
                to visualize geographical placement. Defaults to None.
            contour (bool, optional): If `True`, uses contour-type plots. If `False`, 
                uses scatter-type plots. Defaults to True.
            mag (bool, optional): If `True`, plots magnitudes instead of components. 
                Defaults to False.
            dir (bool, optional): If `True`, plots directions instead of components. 
                Defaults to False.
            arrows (bool, optional): If `True`, overlays arrows on top of the magnitudes 
                to show direction. Defaults to False.
            arrow_vars (dict[str, str], optional): A dictionary defining which directional 
                variable to use for plotting arrows on top of a data variable. For example:
                - `{'hs': 'dirp'}`: Plots peak wave direction (`dirp`) on top of significant 
                wave height (`hs`).
                If `None`, no specific arrow variables are defined. Defaults to None.
            rotated (Optional[bool], optional): If `True`, rotates directions and arrows 
                to align with the projection set by the CRS. If `None`, follows the behavior 
                defined by the `proj` argument. Defaults to None.
            sparse (bool, optional): If `True`, plots only a sparse set of arrows (e.g., 
                25 arrows in each direction) instead of plotting arrows at every grid point. 
                Defaults to True.
            show (bool, optional): If `True`, displays the plot immediately. If `False`, 
                the plot is generated but not displayed. Defaults to True.

        Returns:
            None: This method does not return anything. It generates and optionally displays 
            a plot of the data.

        Examples:
            Basic usage:
            >>> skeleton.quicklook()

            Forcing a longitude-latitude projection:
            >>> skeleton.quicklook(proj='lonlat')

            Comparing with another Skeleton instance:
            >>> skeleton.quicklook(compare=other_skeleton)

            Plotting magnitudes with directional arrows:
            >>> skeleton.quicklook(mag=True, arrows=True, arrow_vars={'hs': 'dirp'})

            Plotting directional arrows from the `dirp` variable (has to have a `dir_type` e.g. from a geo-parameter) on top of the `hs` variable:
            >>> skeleton.quicklook(arrow_vars={'hs': 'dirp'})

        """
        try:
            import matplotlib.pyplot as plt
        except ImportError as e:
            print(f"Quicklook required matplotlib")
            raise e


        if compare is not None:
            if proj == 'xy' or proj is None and self.core.is_projected():
                xedge, yedge = compare.xy(crs=self.proj.crs())
            else:
                xedge, yedge = compare.lonlat()

        arrow_vars = arrow_vars or {}
        vars = self._determine_quicklook_variables(mag, dir, arrows,arrow_vars)

        # No data to plot: only plot points
        
        if not vars:
            if proj is None:
                x, y = self.xy(native=True)
            elif proj =='lonlat':
                x, y = self.lonlat()
            elif proj == 'xy':
                x, y = self.xy()
            plt.scatter(x,y)
            if compare is not None:
                plt.scatter(xedge, yedge,c='k',s=0.5, label=f'{compare.name}')
                plt.legend()
            if show:
                plt.show()
            return

        cols = int(np.ceil(len(vars)**0.5))
        rows = int(np.ceil(len(vars)/cols))


        if coastline:
            try:
                from cartopy import feature as cfeature
                from cartopy import crs as ccrs
            except ImportError as e:
                print(f"Coastlines require cartopy")
                raise e
            if proj == 'lonlat':
                plot_proj = ccrs.PlateCarree()
            elif proj == 'xy':
                if isinstance(self.proj.crs(), tuple):
                    plot_proj = ccrs.UTM(self.proj.crs()[0])
                else:
                    raise NotImplementedError('Coastlines impemented only for lon-lat and UTM')
            else:
                if not self.core.is_projected():
                    plot_proj = ccrs.PlateCarree()
                else:
                    if isinstance(self.proj.crs(), tuple):
                        plot_proj = ccrs.UTM(self.proj.crs()[0])
                    else:
                        raise NotImplementedError('Coastlines impemented only for lon-lat and UTM')
            fig, ax = plt.subplots(rows, cols,subplot_kw={"projection": plot_proj})
        else:
            fig, ax = plt.subplots(rows, cols)
        
        
        ax = np.atleast_2d(ax)
        
        if rotated is None:
            if proj == 'xy':
                rotated = True
            elif proj == 'lonlat':
                rotated = False
            elif self.core.is_projected():
                rotated = True
            else:
                rotated = False

        r, c = 0, 0
        for var, var_dict in vars.items():
            try:
                data = self.get(var, rotated=rotated)
                wrt = ' with respect to CRS' if rotated else ''
            except ProjectionError:
                data = self.get(var, rotated=False)
                wrt = ''
            if not var_dict.get('is_arrow', False):
                if 'time' in self.core.coords():
                    data = data[0,...]
                        
                # Set possible limits if plotting direction as magnitude
                cmap = var_dict.get('cmap','viridis') 
                if cmap == 'twilight':
                    if self.core.meta_parameter(var).dir_type() in  ['to', 'from']:
                        vlim = (0, 360)
                    else:
                        vlim = (-np.pi, np.pi)
                else:
                    vlim = (None, None)

                ax[r,c], cont = self._quicklook(ax[r,c], data, proj, contour, cmap, vlim=vlim) # Implementation varies for GriddedSkeleton and PointSkeleton

                arrow_var = var_dict.get('arrow_data', '')
                if arrow_var:
                    wrt = ' with respect to CRS' if rotated else ''
                    arrow_data = self.get(arrow_var, dir_type='math', rotated=rotated)
                    if 'time' in self.core.coords():
                        arrow_data = arrow_data[0,...]
                    ax[r,c] = self._quicklook_quiver(ax[r,c], arrow_data, proj, arrow_var, wrt, sparse) # Implementation varies for GriddedSkeleton and PointSkeleton
                    ax[r,c].legend(loc='upper right')
                    wrt = ''
            else:
                arrow_data = self.get(var, dir_type='math', rotated=rotated)
                wrt = ' with respect to CRS' if rotated else ''
                if 'time' in self.core.coords():
                    arrow_data = arrow_data[0,...]
                ax[r,c] = self._quicklook_quiver(ax[r,c], arrow_data, proj, var, wrt, sparse) # Implementation varies for GriddedSkeleton and PointSkeleton
                cont = None
                ax[r,c].legend(loc='upper right')
                wrt = ''
            
            
            if proj is None:
                ax[r,c].set_xlabel(self.core.x_str)
                ax[r,c].set_ylabel(self.core.y_str)
            elif proj == 'lonlat':
                ax[r,c].set_xlabel('longitude')
                ax[r,c].set_ylabel('latitude')
            elif proj == 'xy':
                ax[r,c].set_xlabel('x')
                ax[r,c].set_ylabel('y')

            title_str = f"{self.name}"
            if 'time' in self.core.coords():
                title_str += f": {self.time(datetime=False)[0]}"
            
            ax[r,c].set_title(title_str)
            if cont is not None:
                cbar = fig.colorbar(cont, ax=ax[r, c])
                param = self.core.meta_parameter(var)
                if param is not None:
                    units = param.units()
                    if param.dir_type() == 'to':
                        units += ' to'
                    elif param.dir_type() == 'from':
                        units += ' from'
                else:
                    units = '?'
                cbar.set_label(f"{var} [{units}]{wrt}")
            if compare is not None:
                ax[r,c].scatter(xedge, yedge,c='k',s=0.5, label=f'{compare.name}')
                plt.legend()
            
            if coastline:
                ax[r,c].add_feature(
                    cfeature.NaturalEarthFeature(
                "physical", "coastline", "10m", facecolor="none", edgecolor="black"
                )
                )

            c += 1
            if c > cols-1:
                c = 0
                r += 1


        if show:
            plt.show()

    def absorb(self, skeleton_to_absorb: "Skeleton", dim: str) -> "Skeleton":
        """Absorbs another Skeleton object along a specified dimension.

        This method combines the data from the current Skeleton instance with the 
        data from another Skeleton instance (`skeleton_to_absorb`) along the specified 
        dimension (`dim`). For `PointSkeleton` instances, if `dim='inds'` is provided, 
        the `inds` variable is reorganized to account for the absorbed data.

        Args:
            skeleton_to_absorb ("Skeleton"): The Skeleton instance to be absorbed into the current instance.
            dim (str): The dimension along which the absorption will take place. 
                - For `PointSkeleton`, if `dim='inds'`, the `inds` variable of the 
                absorbed Skeleton is adjusted to align with the existing `inds`.

        Returns:
            Skeleton: A new Skeleton instance that combines the data from both Skeletons 
            along the specified dimension.

        Notes:
            - The method uses `xarray.concat` to combine the datasets of both Skeletons 
            along the given dimension, ensuring that data variables are minimally 
            concatenated (`data_vars="minimal"`).
            - The resulting Skeleton is sorted by the specified dimension for consistency.
            - If the Skeleton is not gridded and `dim='inds'`, the `inds` variable of 
            `skeleton_to_absorb` is reorganized by appending the indices to match the existing `inds` of the current Skeleton.

        Examples:
            Absorbing one Skeleton into another along the 'time' dimension:
            >>> new_skeleton = skeleton.absorb(other_skeleton, dim='time')

            For a `PointSkeleton`, reorganizing the `inds` dimension:
            >>> new_skeleton = skeleton.absorb(other_skeleton, dim='inds')
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

    def cut_to_common_times(self, skeleton_to_compare_with: "Skeleton") -> tuple["Skeleton", "Skeleton"]:
        """Restricts the Skeletons to their common time values.

        This method trims the current Skeleton and a provided Skeleton (`skeleton_to_compare_with`) 
        so that both cover only the time instances that are present in both Skeletons. It ensures 
        that the resulting Skeletons have identical time dimensions.

        Args:
            skeleton_to_compare_with ("Skeleton"): The Skeleton instance to compare with. 
                This Skeleton is trimmed to the common time values shared with the current Skeleton.

        Returns:
            tuple["Skeleton", "Skeleton"]: A tuple containing two Skeleton instances:
                - The first is the trimmed version of the current Skeleton.
                - The second is the trimmed version of `skeleton_to_compare_with`.
            Both Skeletons will have identical time dimensions.

        Raises:
            SkeletonError: If either the current Skeleton or `skeleton_to_compare_with` 
            does not have a time dimension.

        Notes:
            - The method ensures that both Skeletons are restricted to the intersection 
            of their respective time dimensions.
            - The comparison is performed using the `time` coordinate of the Skeletons.

        Examples:
            Cutting two Skeletons to their common time values:
            >>> skeleton1, skeleton2 = skeleton1.cut_to_common_times(skeleton2)

            After this operation, both `skeleton1` and `skeleton2` will have identical 
            times, allowing for direct comparisons or further operations.

        """
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

    def _determine_slice_inds(self, x_slice, y_slice, x: str, y: str):
        """Determines the indeces of e.g. a lon-slice for a PointSkeleton"""
        if x_slice is None and y_slice is None:
            return None
        
        x_not_a_slice = x_slice is not None and not isinstance(x_slice, slice)
        y_not_a_slice = y_slice is not None and not isinstance(y_slice, slice)

        if x_not_a_slice and y_not_a_slice and len(np.atleast_1d(x_slice)) == len(np.atleast_1d(y_slice)):
            inds_dict = self.yank_point(**{x: x_slice, y: y_slice})
            return inds_dict["inds"]

        x_inds = _determine_inds(x_slice, self.get(x))
        y_inds = _determine_inds(y_slice, self.get(y))

        return np.array(list(set(x_inds).intersection(set(y_inds))))
        
    def _determine_var_slice_inds(self, name, var_slice):
        """Determines the indeces to slice using variable values if variable is 1D"""
        var_inds = _determine_inds(var_slice, self.get(name))
        return var_inds


    def sel(self, **kwargs) -> "Skeleton":
        """Creates a new Skeleton instance by selecting subsets of the data based on specified criteria.

        This method enables slicing and subsetting of the Skeleton's data using coordinates 
        or variables. It internally calls the xarray `.sel` or `.isel` method to perform the selection on 
        the underlying xarray Dataset. Additionally, it supports slicing using variables with 
        only one non-trivial dimension, making it more flexible for datasets with non-standard 
        coordinates. 
        
        Especially it allows for slicing with lon/lat in unstructured data that is defined over and `inds` coordinate.

        Args:
            **kwargs: Keyword arguments specifying the selection criteria. These can include:
                - Coordinate-based slicing (e.g., `lon=slice(10, 20)` selects longitudes between 10 and 20).
                - Variable-based slicing for variables with only one non-trivial dimension.
                For example, if a variable `temperature` has only one dimension (`time`), 
                you can slice it directly (e.g., `temperature=slice(300, 310)`).

        Returns:
            Skeleton: A new Skeleton instance containing only the selected subset of data.

        Notes:
            - If the Skeleton is not gridded, slicing for longitude (`lon`), latitude (`lat`), 
            x-coordinates (`x`), or y-coordinates (`y`) is handled by determining the corresponding 
            indices (`inds`) first.
            - The method supports variable-based slicing for variables that have only one 
            non-trivial dimension. In such cases:
                - The method determines the intersection of the slicing indices for all 
                specified variables.
                - The resulting subset is based on the shared indices of those variables.
            - If a `time` coordinate is being sliced, the method internally switches to 
            `.isel()` to handle the selection by indices.
            - The method ensures that the resulting subset is returned as a new Skeleton instance.

        Examples:
            Selecting by coordinate ranges:
            >>> new_skeleton = skeleton.sel(lon=slice(10, 20), lat=slice(-5, 5))

            Selecting by a variable with one dimension (e.g., `temperature` based on `time`):
            >>> new_skeleton = skeleton.sel(temperature=slice(300, 310))

            Combining coordinate and variable-based slicing:
            >>> new_skeleton = skeleton.sel(lon=slice(10, 20), temperature=slice(300, 310))

            Selecting by indices for non-gridded Skeletons:
            >>> new_skeleton = skeleton.sel(inds=[1, 2, 3])

        Raises:
            - If a variable specified in the selection criteria has more than one non-trivial dimension, 
            it will not be sliced, as such slicing is not supported by this method.

        Implementation Details:
            - For non-gridded Skeletons, slicing coordinates like `lon` or `lat` involves 
            computing the corresponding `inds` (indices) using `_determine_slice_inds`.
            - Variable-based slicing is handled by identifying the intersection of indices 
            where the slicing condition is met.
            - For variables with a single non-trivial dimension, the method ensures that the 
            selection works seamlessly by restricting the data to the shared indices.
            - If the selection involves the `time` coordinate, the method switches to using 
            `.isel()` with the computed indices for efficiency and compatibility with xarray's methods.

        """
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

            var_inds = None
            all_inds = None
            slicing_coord = ''
            new_kwargs = {}
            for key, value in kwargs.items():
                if key in self.core.non_coord_objects() and len(self.shape(key, squeeze=True)) == 1:
                    all_inds = np.array(range(self.shape(key, squeeze=True)[0]))
                    slicing_coord = self.coord_squeeze(self.core.coords(self.core.coord_group(key)))
                    found_inds = self._determine_var_slice_inds(key, value)
                    if var_inds is None:
                        var_inds = found_inds
                    else:
                        var_inds = np.array(list(set(found_inds).intersection(set(var_inds))))
                else:
                    new_kwargs[key] = value

            if slice_inds is None:
                inds = all_inds if all_inds is not None else self.inds()
            else:
                inds = slice_inds

            if var_inds is not None:
                inds = np.array(list(set(inds).intersection(set(var_inds))))
            
            
            if slicing_coord == ['time']:
                return self.isel(time=inds, **new_kwargs)
            else:
                if len(inds) != self.nx():
                    return self.sel(inds=inds, **new_kwargs)

        return self.from_ds(
            self.ds().sel(**kwargs),
            data_vars=self.core.non_coord_objects(),
            keep_ds_names=True,
            name=self.name,
        )

    def isel(self, **kwargs) -> "Skeleton":
        """Creates a new Skeleton instance by selecting subsets of the data using indices.

        This method enables subsetting of the Skeleton's data by specifying indices for 
        the desired dimensions or variables. It internally calls the xarray `.isel` 
        method on the underlying xarray Dataset to perform the selection. Additionally, 
        it supports slicing using variables with only one non-trivial dimension, based 
        on their indices.

        Args:
            **kwargs: Keyword arguments specifying the selection criteria. These can include:
                - Index-based slicing for dimensions (e.g., `time=[0, 1, 2]` to select the 
                first three time steps).
                - Variable-based slicing for variables with only one non-trivial dimension 
                (e.g., `temperature=[0, 4, 7]` to select specific indices of the variable 
                `temperature`).

        Returns:
            Skeleton: A new Skeleton instance containing only the selected subset of data.

        Notes:
            - Just like the `sel` method, this method supports variable-based slicing for 
            variables with only one non-trivial dimension. When multiple variables are 
            specified, their indices are combined by taking the intersection of indices 
            where the slicing criteria are met.
            - For non-gridded Skeletons, the method can handle slicing for dimensions like 
            `inds` by determining the appropriate indices first.
            - The method ensures that the resulting subset is returned as a new Skeleton 
            instance, preserving the structure and metadata of the original Skeleton.

        Examples:
            Selecting by indices for a specific dimension:
            >>> new_skeleton = skeleton.isel(time=[0, 1, 2])

            Selecting by indices for a variable with one dimension:
            >>> new_skeleton = skeleton.isel(temperature=[0, 4, 7])

            Combining index-based slicing for coordinates and variables:
            >>> new_skeleton = skeleton.isel(time=[0, 1, 2], temperature=[0, 4, 7])

            Selecting specific indices for non-gridded Skeletons:
            >>> new_skeleton = skeleton.isel(inds=[0, 3, 5])
        """
        ds = self.ds().isel(**kwargs)
        expandable_dims = ['x','y','lon','lat'] if self.is_gridded() else ['inds']
        
        for dim in expandable_dims:
            if dim not in ds.dims:
                ds=ds.expand_dims(dim)
        return self.from_ds(
            ds,
            data_vars=self.core.non_coord_objects(),
            keep_ds_names=True,
            name=self.name,
        )

    def insert(self, name: str, data: np.ndarray, **kwargs) -> None:
        """Inserts a slice of data into the Skeleton.

        This method allows you to insert a subset of data into an existing variable in the Skeleton 
        based on the specified coordinate values. The data being inserted must match the shape of 
        the remaining dimensions of the target variable after slicing along the specified coordinates.

        Args:
            name (str): The name of the variable in the Skeleton where the data will be inserted.
            data (np.ndarray): The data slice to insert. The (non-trivial) shape of this data must match the 
                dimensions of the target variable determined by coordinate values (`**kwargs`).
            **kwargs: Keyword arguments specifying the coordinate values for the data slice. 
                These coordinate values are used to determine where in the variable the data 
                should be inserted. For example, for a variable with dimensions 
                (`time`, `inds`, `threshold`), you can specify values for `time` and `threshold`.

        Returns:
            None: This method modifies the Skeleton in place by inserting the data slice 
            into the specified variable.

        Notes:
            - The `**kwargs` must include coordinate values that uniquely identify the slice 
            in the target variable where the data will be inserted.
            - If a coordinate value is provided, its index within the corresponding dimension 
            is determined using `np.where`.
            - The method internally calls `ind_insert` to perform the actual insertion.

        Examples:
            Inserting a data slice into a variable with dimensions ('time', 'inds', 'threshold'):

            If a variable named `geodata` has dimensions ('time', 'inds', 'threshold') with shape (57, 10, 3), 
            and you have a data slice with `threshold=0.4` and `time='2023-11-08 12:00:00'`, having shape (10,), 
            you can insert this slice as follows:

            >>> skeleton.insert(
            >>>     name='geodata',
            >>>     data=data_slice,
            >>>     time='2023-11-08 12:00:00',
            >>>     threshold=0.4
            >>> )
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
        """Inserts a slice of data into the Skeleton using index-based coordinates.

        This method allows you to insert a subset of data into an existing variable in the Skeleton 
        using indices to specify the target location. The data being inserted must match the shape of 
        the remaining dimensions of the target variable after slicing along the specified indices.

        Args:
            name (str): The name of the variable in the Skeleton where the data will be inserted.
            data (np.ndarray): The data slice to insert. The (non-trivial) shape of this data must match the 
                dimensions of the target variable determined by coordinate values (`**kwargs`).
            **kwargs: Keyword arguments specifying the index values for the relevant dimensions 
                of the variable. For example, for a variable with dimensions 
                (`time`, `inds`, `threshold`), you can specify index values for `time` and `threshold`.

        Returns:
            None: This method modifies the Skeleton in place by inserting the data slice 
            into the specified variable.

        Notes:
            - The target variable (`name`) must already exist in the Skeleton.
            - The `**kwargs` must include index values (e.g., integers) that uniquely identify 
            the slice in the target variable where the data will be inserted.
            - This method assumes that the provided indices are valid and within the bounds of 
            the target variable's dimensions.
            - Unlike `insert`, this method uses direct indices instead of coordinate values.

        Examples:
            Inserting a data slice into a variable with dimensions ('time', 'inds', 'threshold'):

            If a variable named `geodata` has dimensions ('time', 'inds', 'threshold') with shape (57, 10, 3), 
            and you have a data slice for the first `threshold` and the first `time` (e.g., shape (10,)), 
            you can insert this slice as follows:

            >>> skeleton.ind_insert(
            >>>     name='geodata',
            >>>     data=data_slice,
            >>>     time=0,
            >>>     threshold=0
            >>> )

            Here, `time=0` and `threshold=0` specify the indices for the dimensions `time` and `threshold`.
        """

        coord_group = self.core.coord_group(name)
        dims = self.core.coords(coord_group)
        index_list = list(np.arange(len(dims)))
        
        wrong_dims = set(kwargs.keys()) - set(dims)
        if wrong_dims:
            raise KeyError(f"Variable {name} doesn't depend on coordinates {wrong_dims}")
        
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
        data: Optional[Union[np.ndarray, xr.DataArray, da.Array]] = None,
        dir_type: Optional[str] = None,
        allow_reshape: bool = True,
        allow_transpose: bool = False,
        fit_to_data: bool = False, 
        coords: Optional[list[str]] = None,
        silent: bool = True,
        chunks: Optional[Union[tuple, str]] = None,
    ) -> None:
        """Sets or updates the data for a variable in the Skeleton.

        This method allows you to set or update the data for a variable in the Skeleton 
        with flexible handling of reshaping, transposing, and directional conventions. 
        It supports data provided as NumPy arrays, xarray DataArrays, or dask arrays, 
        and offers options to reshape or adjust the data to fit the variable's dimensions.

        Args:
            name (Union[str, MetaParameter]): The name of the variable to set or update.
            data (Optional[Union[np.ndarray, xr.DataArray, da.Array]], optional): The data to set for the variable. 
                - If `None`, an empty array is set. Defaults to None.
            dir_type (Optional[str], optional): Defines the directional convention of the data when setting 
                a directional variable. Can be one of:
                - `'from'`: Directions are interpreted as coming from the specified angle in degrees.
                - `'to'`: Directions are interpreted as pointing to the specified angle in degrees.
                - `'math'`: Directions follow standard mathematical conventions using radians.
                If `None`, it assumes the convention already associated with the variable's `dir_type`. 
                Defaults to None.
            allow_reshape (bool, optional): If `True`, allows reshaping of data by squeezing or expanding 
                trivial dimensions. For example:
                - `(10, 1, 10)` → `(10, 10)`
                - `(100,)` → `(100, 1)`
                Defaults to True.
            allow_transpose (bool, optional): If `True`, allows transposing of exactly two non-trivial 
                dimensions, if required. For example:
                - `(8, 3)` → `(3, 8)`
                - `(5, 1, 10)` → `(10, 1, 5)`
                Defaults to False.
            fit_to_data (bool, optional): If `True`, reshapes the data to fit the variable's dimensions 
                automatically. For example:
                - `(100,)` → `(10, 10)`
                Defaults to False.
            coords (Optional[list[str]], optional): A list of coordinate names (e.g., `['freq', 'inds']`) 
                specifying the order of the dimensions in the provided data. If provided, the data will 
                be reshaped accordingly. If `None`, the method attempts to infer the coordinates automatically. 
                Only non-trivial dimensions need to be identified.
                Defaults to None. 
            silent (bool, optional): If `True`, suppresses output messages about any reshaping or adjustments 
                performed. Defaults to True.
            chunks (Optional[Union[tuple, str]], optional): If specified, the data is converted to a dask array 
                with the given chunking strategy. If `dask-mode` is activated using `.dask.activate()`, any 
                NumPy array is automatically converted to a dask array. Defaults to None.

        Returns:
            None: This method modifies the Skeleton in place by setting the data for the specified variable.

        Notes:
            - **Directional Convention (`dir_type`)**:
            When setting directional variables, the `dir_type` argument allows you to specify the 
            convention of the provided data. For example, if a variable `dirp` has a `dir_type='from'`, 
            the following are equivalent:
            1. `skeleton.set('dirp', 0)`
            2. `skeleton.set('dirp', 0, dir_type='from')`
            3. `skeleton.set('dirp', 180, dir_type='to')`
            - **Reshaping Logic**:
            1. If `coords` is provided, the data is reshaped assuming the dimensions are ordered as specified 
                in `coords`. Only non-trivial dimensions need to be identified, as othersa are squeezed/expanded.
            2. If `data` is a DataArray, the coordinates are inferred automatically.
            3. Trivial dimensions (e.g., dimensions of size 1) are automatically squeezed.
            4. If any trivial dimensions are missing from the data, they are expanded.
            - **Dask Handling**:
            - If `chunks` is specified, any NumPy array is converted to a dask array with the specified chunking.
            - If the provided data is already a dask array, it is used as-is without rechunking.

        Examples:
            Setting data for a variable with size (10,10) with reshaping:
            >>> skeleton.set('temperature', data=np.random.rand(10, 1, 10))

            Setting data for a directional variable with a specific convention:
            >>> skeleton.set('dirp', data=0, dir_type='from')

            Automatically reshaping data to fit variable variable with size (10,10):
            >>> skeleton.set('geodata', data=np.random.rand(100), fit_to_data=True)

            Reshaping data based on specified coordinates if data in skeleton is size (10,10) but defined over ['inds', 'freq']:
            >>> skeleton.set('geodata', data=np.random.rand(10, 10), coords=['freq', 'inds'])
        """
        if self.ds() is None:
            raise(MissingDatasetError)
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
            fit_to_data, 
        )

        if dir_type not in ["to", "from", "math", None]:
            raise DirTypeError(
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
            self.meta._metadata_to_ds(name)
            if self.core.get(name).coord_group in ['all', 'spatial', 'grid']:
                if self.core.is_projected():
                    self.meta.append({'grid_mapping': 'crs'}, name)
                else:
                    self.meta.append({'grid_mapping': 'wgs84'}, name)
        return

    def _reshape_data(
        self,
        name: str,
        data: np.ndarray,
        coords: list[str],
        silent: bool,
        allow_reshape: bool,
        allow_transpose: bool,
        fit_to_data: bool, 
    ) -> np.ndarray:
        """Reshapes the data using the following logic:

        allow_reshape [True]: Allow squeezing out trivial dimensions.
        allow_transpose [False]: Allow trying to transpose exactly two non-trivial dimensions

        Otherwise, data is assumed to be in the right dimension, but can also be reshaped:

        1) If 'coords' (e.g. ['freq',' inds']) is given, then data is reshaped assuming data is in that order.
        2) If data is a DataArray, then 'coords' is set using the information in the DataArray.
        3) If data has any trivial dimensions, then those are squeezed.

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
        if data is None:
            raise DataWrongDimensionError(self.shape(name), len(coords))
        if data.shape != self.shape(name):
            # If we are here then the data could not be set, but we are allowed to try to reshape
            if not silent:
                print(f"Size of {name} does not match size of {type(self).__name__}...")
            # Save this for messages
            original_data_shape = data.shape

            if fit_to_data:
                data = reshape_manager.fit_to_shape(
                    data, wanted_shape=self.size(coord_type)
                )

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
        if obj.direction is None:
            raise SkeletonError(f"Cannot set a magnitude '{name}' that has no associated direction! Set componensts '{obj.x}' and '{obj.y}' separatesly, or modify class to have an associated direction.")
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
        set_dir_type = self.core.get_dir_type(name)
        if dir_type is not None and set_dir_type is None:
            raise DirTypeError

        dir_type = dir_type or set_dir_type
        data = dir_conversions.convert(data, in_type=dir_type, out_type=set_dir_type)
        self._ds_manager.set(data=data, name=name)
        self.meta._metadata_to_ds(name)
        self._trigger_masks(name, data)

    def _trigger_masks(self, name: str, data: Union[np.ndarray, xr.DataArray]) -> None:
        """Set any masks that are triggered by setting a specific data variable
        E.g. Set new 'land_mask' when 'topo' is set."""
        for mask in self.core._triggers(name):

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
        rotated: bool=False,
        verbose: bool=False,
        **kwargs,
    ) -> Union[np.ndarray, xr.DataArray, da.Array]:
        """Retrieves a mask or data variable as an array.

        This method retrieves the data for a specified variable from the Skeleton instance, 
        with options to control the data format, dimensionality, and projection. The returned 
        data can be a NumPy array, xarray DataArray, or dask array, depending on the input 
        arguments.

        Args:
            name (str): The name of the variable to retrieve.
            strict (bool, optional): If `True`, returns `None` if the data is not set. 
                If `False`, returns an empty array (i.e. filled with default values) if the variable is unset. 
                Defaults to False.
            empty (bool, optional): If `True`, returns an array filled with default values, 
                even if the variable is already set. Defaults to False.
            data_array (bool, optional): If `True`, returns the data as an xarray DataArray. 
                If `False`, returns the data as a NumPy or dask array. Defaults to False.
            dir_type (Optional[str], optional): Defines the directional convention of the 
                returned data if the variable is directional. Can be one of:
                - `'from'`: Directions are interpreted as coming from the specified angle in degrees.
                - `'to'`: Directions are interpreted as pointing to the specified angle in degrees.
                - `'math'`: Directions follow standard mathematical conventions in readians.
                If `None`, it gives data in the convention already associated with the variable's `dir_type`. 
                Defaults to None.
            squeeze (bool, optional): If `True`, removes trivial dimensions (e.g., dimensions 
                of size 1) but ensures that the result is at least a 1D array. Defaults to True.
            dask (Optional[bool], optional): Determines the type of array to return:
                - `True`: Returns a dask array.
                - `False`: Returns a NumPy array.
                - `None`: Uses the Skeleton's current dask mode. Defaults to None.
            rotated (bool, optional): If `True`, rotates the data to align with the set CRS 
                projection (e.g., UTM or rotated pole). Defaults to False.
            **kwargs: Additional arguments for filtering or subsetting the data.

        Returns:
            Union[np.ndarray, xr.DataArray]: The requested variable as either a NumPy array, 
            dask array, or xarray DataArray, depending on the input arguments.

        Notes:
            - **Directional Convention (`dir_type`)**: When retrieving directional variables, 
            the `dir_type` argument allows you to specify the desired convention of the 
            returned data. For example:
            1. If a variable `dirp` has a `dir_type='from'`, and a value of 0:
                - `skeleton.get('dirp', dir_type='from')` → Returns 0 degrees.
                - `skeleton.get('dirp', dir_type='to')` → Returns 180 degrees.
            - **Squeezing Dimensions**: If `squeeze=True`, trivial dimensions (size 1) are removed, 
            but the result will always be at least 1D. Spatial coordinates (e.g., `lon/lat` or `x/y`) 
            are never squeezed away unless a non-trivial coordinate is present.

            Examples:
                - If `data` has shape `(10, 10, 1)`, the result is `(10, 10)`.
                - If `data` is gridded has shape `[1, 2, 1]`, the result is `(2,)` (non-trivial spatial coordinate preserved).
                - If `data` is gridded on one point and has shape `(1, 1)`, the result is `(1, 1)` 
                (spatial coordinates are preserved).
                - If `data` is not gridded on one point and has shape `(1,)`, the result is `(1,)`.
                - If `data` is gridded on one point with a `time` dimension having two time stamps 
                (e.g., `time` and `lon/lat`), and the data has shape `(2, 1, 1)`, the result is `(2,)` (no spatial coordinate preserved).
                - If `data` is not gridded on one point with a `time` dimension having two time 
                stamps (e.g., `time` only), and the data has shape `(2, 1)`, the result is `(2,)` (no spatial coordinate preserved).
            - **Handling Unset Variables**:
            - If `strict=True` and the variable is unset, the method returns `None`.
            - If `strict=False` and the variable is unset, an empty array is returned.
            - **Dask Mode**:
            - If `dask=True`, the method ensures the returned data is a dask array.
            - If `dask=False`, the method ensures the returned data is a NumPy array.
            - If `dask=None`, the method uses the Skeleton's current dask mode.

        Examples:
            Retrieving a variable as a NumPy array:
            >>> data = skeleton.get('temperature')

            Retrieving a variable as an xarray DataArray:
            >>> data = skeleton.get('temperature', data_array=True)

            Retrieving a directional variable with a specific convention:
            >>> dir_data = skeleton.get('dirp', dir_type='to')

            Retrieving data with trivial dimensions squeezed are not squeezed:
            >>> data = skeleton.get('temperature', squeeze=False)

            Retrieving rotated data:
            >>> rotated_data = skeleton.get('wind', rotated=True)

            Handling unset variables:
            >>> data = skeleton.get('unknown_var', strict=True)  # Returns None if not set
            >>> data = skeleton.get('unknown_var', strict=False)  # Returns an array filled with default values

        """
        if self.ds() is None:
            raise MissingDatasetError

        if not isinstance(name, str) and not gp.is_gp(name):
            raise TypeError(
                f"'name' must be of type 'str', or 'MetaParameter' not '{type(name).__name__}'!"
            )

        if gp.is_gp(name):
            if verbose:
                print(f"Requested geo-parameter {name}")
            
            if name.dir_type() is None:
                names = self.core.find(name)

            else:
                # Allows getting with e.g. gp.wave.DirpTo when skeleton has gp.wave.Dirp
                for key, var in name.my_family().items():
                    if key in {'direction', 'opposite_direction'}:
                        names = self.core.find(var)
                        if len(names) > 0:
                            if verbose:
                                print(f"Found directional geo-parameter {var} in dataset")
                            if dir_type is None:
                                dir_type = name.dir_type()
                                if verbose:
                                    print(f"'dir_type' not specified, setting to '{dir_type}'")
                            break
            
            
            if len(names) == 0:
                raise UnknownVariableError(f"Variable matching {name} not found!")
            if len(names) > 1:
                raise UnknownVariableError(
                    f"Found several variables ({names}) matching {name}!"
                )
            name = names[0]
            if verbose:
                print(f"Reading variable '{name}'")

        if name == "x":
            return self.x(strict=strict, **kwargs)
        elif name == "y":
            return self.y(strict=strict, **kwargs)
        elif name == "lon":
            return self.lon(strict=strict, **kwargs)
        elif name == "lat":
            return self.lat(strict=strict, **kwargs)

        if dir_type not in ["to", "from", "math", None]:
            raise DirTypeError(
                f"'dir_type' needs to be 'to', 'from' or 'math' (or None), not {dir_type}"
            )

        if name in self.core.magnitudes():
            if dir_type:
                raise DirTypeError
            if rotated:
                raise ProjectionError('Cannot rotate a magnitude!')
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
                rotated=rotated,
                **kwargs,
            )
        elif name in self.core.mask_points():
            if rotated:
                raise ProjectionError('Cannot rotate mask points!')
            lon, lat = eval(f"self.{name}(strict=strict, **kwargs)")
            return lon, lat

        elif name in self.core.masks():
            if rotated:
                raise ProjectionError('Cannot rotate a mask!')
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
        elif self.core.get_dir_type(name) is not None: # Directional variable
            if rotated:
                data = self._get_data(
                    name=name,
                    strict=strict,
                    dir_type='math',
                    empty=empty,
                    **kwargs,
                )
                x_data = dask_computations.cos(data)
                y_data = dask_computations.sin(data)
                lon, lat = self.lonlat()
                x_data, y_data = self.proj._rotate_u_v(x_data, y_data, lon=lon, lat=lat,grid_shape=self.size('spatial'))
                math_dir = dir_conversions.compute_math_direction(x_data, y_data)
                data = dir_conversions.convert_from_math_dir(math_dir, dir_type=dir_type or self.core.get_dir_type(name))
            else:
                data = self._get_data(
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

            if rotated:
                twin_name = self.core.find_twin_component(name)
                twin_data = self._get_data(
                    name=twin_name,
                    strict=strict,
                    dir_type=dir_type,
                    empty=empty,
                    **kwargs,
                )
                my_param = self.core.meta_parameter(name)
                twin_param = self.core.meta_parameter(twin_name)
                if twin_param is None:
                    raise ProjectionError(f"Cannot find orthogonal component to '{name}'!")
                lon, lat = self.lonlat()
                if my_param.i_am() =='x':
                    data, __ = self.proj._rotate_u_v(data, twin_data, lon=lon, lat=lat, grid_shape=self.size('spatial'))
                elif my_param.i_am() == 'y':
                    __, data = self.proj._rotate_u_v(twin_data, data, lon=lon, lat=lat, grid_shape=self.size('spatial'))
                else:
                    raise ProjectionError(f"'{name}' doesn't seem to be a component!")

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
        else:
            if rotated:
                data = data.assign_attrs({'rotated_according_to':'crs'})

        return data

    def _get_direction(
        self,
        name: str,
        strict: bool,
        empty: bool,
        dir_type: str,
        rotated: bool,
        **kwargs,
    ) -> xr.DataArray:
        x_name = self.core.get(name).x
        y_name = self.core.get(name).y
        x_data = self._ds_manager.get(
            x_name,
            empty=empty,
            strict=strict,
            **kwargs,
        )
        y_data = self._ds_manager.get(
            y_name,
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

        if rotated:
            lon, lat = self.lonlat()
            x_data, y_data = self.proj._rotate_u_v(x_data, y_data, lon=lon, lat=lat,grid_shape=self.size('spatial'))
            

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

        Spatial dims are not generally protected, but if the results is a 0-dim,
        then 'inds' or 'lon'&'lat' or 'x'&'y' is kept."""
        dims_to_drop = [
            dim
            for dim in self.core.coords("all")
            if (dim in data.dims and len(data[dim].values) == 1)
        ]
        
        # If it looks like we are dropping all coords, then save the spatial ones
        if set(dims_to_drop) == set(data.coords):
            dims_to_drop = list(
                set(dims_to_drop) - set(self.core.coords('spatial')) - set([name])
            )

        if dims_to_drop:
            data = data.squeeze(dim=dims_to_drop, drop=True)

        return data

    def coord_squeeze(self, coords: list[str]) -> list[str]:
        """Smartly squeezes a list of coordinates by removing trivial dimensions.

        This method processes a list of coordinates and applies the following rules 
        to "squeeze" the list based on the lengths of the associated data dimensions:

        Rules:
            1) If the input coordinate list is empty, an empty list is returned.
            2) If the input list contains only one coordinate, it is returned as-is.
            3) Coordinates corresponding to trivial dimensions (length 1) are removed.
            If this results in a non-empty list, the remaining coordinates are returned.
            4) If all coordinates are trivial and the resulting list would be empty, the 
            method returns a single default spatial coordinate, based on these priorities:
                - `'inds'`
                - `'lat'` or `'y'` (latitude or y-coordinate)
                - `'lon'` or `'x'` (longitude or x-coordinate)

        Args:
            coords (list[str]): A list of coordinate names to be squeezed.

        Returns:
            list[str]: A squeezed list of coordinates, based on the rules above.

        Notes:
            - A "trivial" coordinate is defined as a coordinate whose associated data 
            has a length of 1.
            - If the list contains spatial coordinates (`inds`, `lat`, `y`, `lon`, or `x`), 
            the method ensures that at least one spatial coordinate is returned, even 
            if all are trivial.
            - The method prioritizes spatial coordinates in the following order:
            `'inds' > 'lat' > 'y' > 'lon' > 'x'`.

        Examples:
            Input list is empty:
            >>> skeleton.coord_squeeze([])
            []

            Input list contains one coordinate:
            >>> skeleton.coord_squeeze(['time'])
            ['time']

            Input list contains trivial coordinates:
            >>> skeleton.coord_squeeze(['time', 'lat', 'lon'])
            ['lat', 'lon']  # Removes 'time' if it is trivial.

            Input list contains only trivial coordinates:
            >>> skeleton.coord_squeeze(['time', 'inds'])
            ['inds']  # Returns 'inds' as the default spatial coordinate.

            Input list contains only spatial coordinates:
            >>> skeleton.coord_squeeze(['x', 'y'])
            ['y']  # Returns 'y' as the primary spatial coordinate.
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

    def ds(self, compile: bool = False, rotated: bool = False) -> Union[xr.Dataset, None]:
        """Returns the underlying xarray Dataset for the Skeleton.

        This method provides access to the underlying xarray Dataset associated with 
        the Skeleton. It can optionally add computed magnitudes and directions or 
        rotate directional data to align with the set coordinate reference system (CRS).

        Args:
            compile (bool, optional): If `True`, adds computed magnitudes and directional 
                variables to the Dataset. Note that this operation performs a deep copy 
                of the data and may be computationally expensive. Defaults to False.
            rotated (bool, optional): If `True`, rotates directional variables to align 
                with the set CRS (e.g., UTM or rotated pole). Defaults to False.

        Returns:
            Union[xr.Dataset, None]: The underlying xarray Dataset. Returns `None` if 
            the Dataset does not exist.

        Notes:
            - If `compile=True`, the method computes and adds magnitudes and directions 
            to the Dataset.
            - If `rotated=True`, the method applies rotation to directional variables 
            (e.g., wind directions) based on the set CRS.

        Examples:
            Retrieving the underlying Dataset:
            >>> dataset = skeleton.ds()

            Retrieving the Dataset with computed magnitudes and directions:
            >>> compiled_dataset = skeleton.ds(compile=True)

            Retrieving the Dataset with rotated directions:
            >>> rotated_dataset = skeleton.ds(rotated=True)

            Combining both options:
            >>> compiled_and_rotated_dataset = skeleton.ds(compile=True, rotated=True)
        """
        if not hasattr(self, "_ds_manager"):
            return None
        ds = self._ds_manager.ds()
        if compile or rotated:
            ds = deepcopy(ds)
            crs = self.proj.crs()
            if isinstance(crs, tuple):
                crs = {'utm_zone': str(crs[0]), 'utm_letter': crs[1]}
            else:
                crs = crs.to_cf()
            ds['crs'].attrs = crs
            if rotated:
                for var in self.core.data_vars():
                    param = self.core.meta_parameter(var)
                    if param is None:
                        continue
                    if param.i_am() in ['x', 'y'] or param.dir_type() is not None:
                        ds[var] = self.get(var, data_array=True, rotated=rotated, squeeze=False)
                
            for mag in self.core.magnitudes():
                ds[mag] = self.get(mag, data_array=True, squeeze=False)
            for dirs in self.core.directions():
                ds[dirs] = self.get(dirs, data_array=True, rotated=rotated, squeeze=False)

        return ds

    def size(
        self, coord_group: str = "all", squeeze: bool = False, **kwargs
    ) -> tuple[int]:
        """Returns the size of the Skeleton for a specified coordinate group.

        This method computes the size (number of elements) of the Skeleton across 
        different groups of coordinates, such as all coordinates, spatial coordinates, 
        grid coordinates, or grid point coordinates. The size is returned as a tuple 
        of integers representing the size of each dimension in the specified group.

        Args:
            coord_group (str, optional): The coordinate group for which to calculate the size. 
                Must be one of:
                - `'all'` (default): Returns the size of the entire Skeleton.
                - `'spatial'`: Returns the size over spatial coordinates (`x`, `y`, `lon`, `lat`, `inds`).
                - `'nonspatial'`: Returns the size over all coordinates that are not spatial.
                - `'grid'`: Returns the size over grid coordinates (e.g., `z`, `time`) combined with spatial coordinates.
                - `'gridpoint'`: Returns the size over grid point coordinates (e.g., `frequency`, `direction`, or `time`).
            squeeze (bool, optional): If `True`, removes trivial dimensions (size 1) from the result. 
                Defaults to `False`.
            **kwargs: Can be used to slice data before calculating the size.

        Returns:
            tuple[int]: A tuple of integers representing the size of each dimension in the 
            specified coordinate group.

        Raises:
            KeyError: If `coord_group` is not one of `'all'`, `'spatial'`, `'grid'`, `'gridpoint'` or `'nonspatial'`.

        Notes:
            - If `squeeze=True`, trivial dimensions are removed from the size computation.
            - Use `coord_group='all'` to get the size of the entire Skeleton, or specify 
            other groups to focus on specific subsets of the coordinates.

        Examples:
            The skeleton is defined over coordinats ['time','lon','lat','freq','dir']
            Of these 'time' is added as a grid coord
            'freq' and 'dir' are added as gridpoint coords

            The size of the data is (1,100,50,25,36)

            Get the size of the entire Skeleton:
            >>> skeleton.size()
            (1,100,50,25,36)

            Get the size over spatial coordinates only:
            >>> skeleton.size(coord_group='spatial')
            (100, 50)

            Get the size over grid coordinates (includes spatial coordinates) `time`, `lon` and `lat`:
            >>> skeleton.size(coord_group='grid')
            (1, 100, 50)

            Get the size over grid point coordinates `freq`, `dir`:
            >>> skeleton.size(coord_group='gridpoint')
            (25, 36)

            Get the size of the entire Skeleton with trivial dimensions removed:
            >>> skeleton.size(coord_group='all', squeeze=True)
            (100,50,25,36)
        """
        if coord_group not in ["all", "spatial", "grid", "gridpoint", 'nonspatial']:
            raise KeyError(
                f"coords should be 'all', 'spatial', 'grid', 'gridpoint' or 'nonspatial', not {coord_group}!"
            )
        coords = self.core.coords(coord_group)
        if squeeze:
            coords = self.coord_squeeze(coords)
        return self._ds_manager.coords_to_size(coords, **kwargs)

    def shape(self, var, squeeze: bool = False, **kwargs) -> tuple[int]:
        """Returns the shape of a specific data variable in the Skeleton.

        This method computes the shape (size of each dimension) of a specific data 
        variable within the Skeleton. If the variable is a coordinate, its shape 
        is retrieved directly. Otherwise, the method calculates the shape based on 
        the coordinate group associated with the variable.

        Args:
            var (str): The name of the data variable for which to compute the shape.
            squeeze (bool, optional): If `True`, removes trivial dimensions (size 1) 
                from the result. Defaults to `False`.
            **kwargs: Can be used to slice data before calculating the size.

        Returns:
            tuple[int]: A tuple of integers representing the shape of the specified 
            data variable.

        Notes:
            - If the variable is a coordinate, its shape is retrieved directly.
            - If `squeeze=True`, trivial dimensions are removed from the shape computation.

        Examples:
            The skeleton is defined over coordinats ['time','lon','lat','freq','dir']
            The variable 'spec' is defined over all coordinates
            The variable 'reference_spec' is defined over 'freq', 'dir'

            The size of the data is (1,100,50,25,36)

            Get the shape of the 'spec' variable:
            >>> skeleton.shape('spec')
            (1,100,50)

            Get the squeezed shape of the 'spec' variable:
            >>> skeleton.shape('spec', squeeze=True)
            (100,50)

            Get the shape of the 'reference_spec' variable:
            >>> skeleton.size('reference_spec')
            (25, 36)
        """
        if var in self.core.coords("all"):
            return self.get(var, squeeze=False).shape
        coord_group = self.core.coord_group(var)
        return self.size(coord_group=coord_group, squeeze=squeeze, **kwargs)

    def inds(self, **kwargs) -> Union[np.ndarray, None]:
        """Returns the index variable for PointSkeletons. Defaults to None for GriddedSkeletons.
        
        **kwargs can be used for slicing"""
        return self.get("inds", **kwargs)

    def edges(
        self,
        coord: str,
        native: bool = False,
        strict: bool = False,
        crs: Optional[CRSValue] = None,
        expansion_factor: Optional[float] = None,
    ) -> tuple[float, float]:
        """Returns the minimum and maximum values (edges) of a specified coordinate.

        This method calculates the minimum and maximum values for a specified coordinate 
        (e.g., `x`, `y`, `lon`, `lat`) within the Skeleton. For spherical grids, it handles 
        conversions and projections if needed. An optional expansion factor can be applied 
        to expand the edges beyond their original range.

        Args:
            coord (str): The coordinate for which to calculate the edges. Must be one of:
                - `'x'`: Cartesian x-coordinate.
                - `'y'`: Cartesian y-coordinate.
                - `'lon'`: Longitude.
                - `'lat'`: Latitude.
            native (bool, optional): If `True`, returns the edges in the native coordinate 
                reference system (CRS) without any transformations. Defaults to `False`.
            strict (bool, optional): If `True`, returns None if edges required in non-native system. 
                Defaults to `False`.
            crs (Optional[CRSValue], optional): The coordinate reference system 
                to use for transformations. Can be specified as an EPSG code (e.g., `4326`), 
                a CRS string, UTM Zone (e.g. (33, 'W')) or a dictionary. If `None`, defaults to the Skeleton's CRS. 
                Defaults to `None`.
            expansion_factor (Optional[float], optional): A factor to expand the edges 
                beyond their original range. For example, an `expansion_factor=1.1` expands 
                the edges by 10% on either side. If `None`, no expansion is applied. 
                Defaults to `None`.

        Returns:
            tuple[float, float]: A tuple representing the minimum and maximum values of the 
            specified coordinate. If the coordinate cannot be determined, returns `(None, None)`.

        Raises:
            KeyError: If the provided `coord` is not one of `'x'`, `'y'`, `'lon'`, or `'lat'`.

        Notes:
            - For Cartesian coordinates (`x`, `y`), the method retrieves the values using 
            the Skeleton's `x` and `y` methods. If these are unavailable, it falls back 
            to the `xy` method.
            - If `native=True`, the edges are returned in the native CRS without conversion.
            - If `expansion_factor` is provided, the edges are expanded symmetrically based 
            on the range of the coordinate values.

        Examples:
            Get the edges of longitude:
            >>> data = GriddedSkeleton(lon=(10, 20), lat=(50, 60)) # Best estimate UTM-zone (32, 'U') set automatically
            >>> data.edges('lon')
            (10.0, 20.0)

            Get the edges of the x-coordinate (Cartesian projection):
            >>> data.edges('x')
            (555776.2667518167, 1287473.8976382306)

            Get the edges of latitude with a 10% expansion factor:
            >>> data.edges('lat', expansion_factor=1.1)
            (49.5, 60.5)

            Attempt to get the edges of the y-coordinate, enforcing strict mode:
            >>> data.edges('y', strict=True)
            (None, None)  # Returns None because the y-coordinate is estimated, not native.

            Get the edges of the x-coordinate in the native CRS, i.e. lon-lat WGS84:
            >>> data.edges('x', native=True)
            (10.0, 20.0)

            Get edges in another projection
            >>> data.edges('x', crs=(33,'W'))
            (141743.63163730752, 858256.3683626924)
        """
        if coord not in ["x", "y", "lon", "lat"]:
            raise KeyError("coord need to be 'x', 'y', 'lon' or 'lat'.")

        if coord in ["x", "y"]:
            x, y = self.x(native=native, strict=strict, crs=crs), self.y(native=native, strict=strict, crs=crs)
            if x is None:
                x, y = self.xy(native=native, strict=strict, crs=crs)
        else:
            x, y = self.lon(native=native, strict=strict, crs=crs), self.lat(native=native, strict=strict, crs=crs)
            if x is None:
                x, y = self.lonlat(native=native, strict=strict, crs=crs)
        
        if coord in ["x", "lon"]:
            val = x
        else:
            val = y
        if val is None:
            return (None, None)

        if expansion_factor:
            length = float(np.max(val))-float(np.min(val))
            pad = length*(expansion_factor-1)/2
            return  (float(np.min(val))-pad, float(np.max(val)+pad))

        return  (float(np.min(val)), float(np.max(val)))
    
    # def extent(self, coord: str, strict: bool = False) -> float:
    #     """Gives the extent in metres in x- or y-direction.

    #     Cartesian grid: The difference between the edges
    #     Spherical grid ['x']: Mean of distance between longitude edges for southern and northern edges
    #     Spherical grid ['y']: Mean of distance between latitude edges for western and eastern edges

    #     Note, that for PointSkeletons the extens is actually a measure of the rectangle covering the points."""
    #     if coord not in ["x", "y",'lon','lat']:
    #         raise KeyError("coord need to be 'x', 'y', 'lon' or 'lat'.")


    #     if not self.core.is_projected() and strict:
    #         return None

    #     if self.core.is_projected():
    #         return np.diff(self.edges(coord))[0]

    #     lon1, lon2 = self.edges("lon")
    #     lat1, lat2 = self.edges("lat")
    #     if coord in ['x','lon']:
    #         d_south = distance_2points(lat1=lat1, lon1=lon1, lat2=lat1, lon2=lon2)
    #         d_north = distance_2points(lat1=lat2, lon1=lon1, lat2=lat2, lon2=lon2)
    #         return (d_south+d_north)/2
    #     else:
    #         d_west = distance_2points(lat1=lat1, lon1=lon1, lat2=lat2, lon2=lon1)
    #         d_east = distance_2points(lat1=lat1, lon1=lon2, lat2=lat2, lon2=lon2)
    #         return (d_west + d_east)/2

    def nx(self) -> int:
        """Length of x/lon-vector."""
        return len(self.x(native=True))

    def ny(self) -> int:
        """Length of y/lat-vector."""
        return len(self.y(native=True))

    def coord_dict(self, coord_group: str = "all") -> dict[str, np.ndarray]:
        """Returns a coord dictionary containing all coordinates of the given coordinate group.

        for 'all' the dict can be used to recreate the Skeleton."""
        coords = list(set(self.core.coords(coord_group)) - set(["inds"]))
        if coord_group in ["all", "spatial", "grid"]:
            coords = coords + self.core.data_vars("spatial")
        return {c: self.get(c) for c in coords}

    def yank_point(
        self,
        lon: Optional[CoordinateValue] = None,
        lat: Optional[CoordinateValue] = None,
        x: Optional[CoordinateValue] = None,
        y: Optional[CoordinateValue] = None,
        unique: bool = False,
        fast: bool = True,
        npoints: int = 1,
        gridded_shape: Optional[tuple[int]] = None,
    ) -> dict[str, np.ndarray]:
        """Finds the nearest points to specified coordinates and returns their indices.

        This method identifies the points in the Skeleton that are closest to the given 
        `x-y` or `lon-lat` coordinates and returns a dictionary containing the indices 
        of the nearest points, as well as the distance to those points. The method supports 
        both `PointSkeleton` and `GriddedSkeleton` and provides flexible options for handling 
        unique points, search speed, and grid structures.

        Args:
            lon (CoordinateValue, optional): Longitude(s) of the point(s) to search for. 
                Defaults to None.
            lat (CoordinateValue, optional): Latitude(s) of the point(s) to search for. 
                Defaults to None.
            x (CoordinateValue, optional): Cartesian x-coordinate(s) of the point(s) 
                to search for. Defaults to None.
            y (CoordinateValue, optional): Cartesian y-coordinate(s) of the point(s) 
                to search for. Defaults to None.
            unique (bool, optional): If `True`, ensures that only unique points are returned 
                (removes duplicates). Defaults to False.
            fast (bool, optional): If `True`, uses UTM Cartesian search for faster computations, 
                particularly at low latitudes.
                Defaults to True, but for points outside valid UTM range falls back to False. 
            npoints (int, optional): The number of nearest points to find. Defaults to 1.
            gridded_shape (Optional[tuple[int]], optional): The shape of the grid (e.g., `(ny, nx)`) 
                to compute `inds_x` and `inds_y` when working with a raveled 2D matrix. 
                This is useful for converting raveled indices back to grid-based indices. Defaults to None.

        Returns:
            dict[str, np.ndarray]: A dictionary containing the indices and distances of the nearest points:
                - `'dx'`: The distance(s) to the nearest point(s) in meters.
                - For `PointSkeleton`: The dictionary includes the key `'inds'` (indices of the closest points).
                - For `GriddedSkeleton`: The dictionary includes the keys `'inds_x'` and `'inds_y'` 
                (grid-based indices of the closest points).

        Notes:
            - Setting `unique=True` removes repeated points from the result, ensuring each returned point is unique.
                Else, the number of returned points match the number of query points, but can have duplicates.
            - Using `fast=True` enables a faster UTM Cartesian search, which is available at lower latitudes.
            - The `gridded_shape` argument is essential when working with raveled grids, as it allows the method 
            to compute the grid-based indices (`inds_x`, `inds_y`) from the raveled indices.

        Examples:
            Example with a `GriddedSkeleton`:
            >>> grid = GriddedSkeleton(lon=(10, 11), lat=(0, 1))
            >>> grid.set_spacing(nx=10, ny=5)
            >>> ind_dict_gridded = grid.yank_point(lon=10.09, lat=0.51)
            >>> print(ind_dict_gridded)
            {'inds_x': array([1]), 'inds_y': array([2]), 'dx': array([2596.57832039])}

            Example with a `PointSkeleton`:
            >>> lon, lat = grid.lonlat() # Ravels points from previous example
            >>> points = PointSkeleton(lon=lon, lat=lat)
            >>> ind_dict = points.yank_point(lon=10.09, lat=0.51)
            >>> print(ind_dict)
            {'inds': array([21]), 'dx': array([2596.57832039])}
            >>> ind_dict = points.yank_point(lon=10.09, lat=0.51, gridded_shape=grid.size())
            >>> print(ind_dict)
            {'inds': array([21]), 'dx': array([2596.57832039]), 'inds_x': array([1]), 'inds_y': array([2])}

            Example finding multiple nearest points:
            >>> ind_dict = grid.yank_point(lon=10.09, lat=0.51, npoints=3)
            >>> print(ind_dict)
            {'inds_x': array([1, 0, 2]), 'inds_y': array([2, 2, 2]), 'dx': array([ 2596.57832039, 10076.86077525, 14756.94307178])}

            Example yanking several points:
            >>> ind_dict = grid.yank_point(lon=(10.09,10.11,10.5), lat=(0.51,0.49,0.8))
             >>> print(ind_dict)
            {'inds_x': array([1, 1, 4]), 'inds_y': array([2, 2, 3]), 'dx': array([2596.57832039, 1112.40474332, 8294.42706169])}
            >>> ind_dict = grid.yank_point(lon=(10.09,10.11,10.5), lat=(0.51,0.49,0.8), unique=True)
             >>> print(ind_dict)
            {'inds_x': array([1, 4]), 'inds_y': array([2, 3]), 'dx': array([2596.57832039, 8294.42706169])}        
        """

        xy_given = x is not None and y is not None
        lonlat_given = lon is not None and lat is not None
        if not xy_given and not lonlat_given:
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
            inds, ii = np.unique(inds, return_index=True)
            dx = np.array(dx)
            dx=dx[ii]

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
        if self.proj.crs() is not None:
            lat = self.proj._lat(x, y, crs=self.proj.crs())
            lon = self.proj._lon(x, y, crs=self.proj.crs())
        else:
            lat, lon = None, None

        inds, dx = self._yank_inds(x, y, lon, lat, self.proj.crs(), fast, npoints)
        return inds, dx

    def _yank_using_lonlat(
        self, lon: np.ndarray, lat: np.ndarray, fast: bool, npoints: int
    ) -> tuple[np.ndarray, np.ndarray]:
        """Finds the indeces of nearest points and distances if lon,lat coordinates are provided"""
        inds = [None] * len(lon)
        dx = [None] * len(lon)
        
        for n, (lo, la) in enumerate(zip(lon, lat)):
            lo, la = np.array([lo]), np.array([la])
            if self.core.is_cartesian():
                crs_to_use = self.proj.crs()
            else:
                crs_to_use = self.proj._optimal_utm(lon=lo, lat=la)
            
            if crs_to_use is not None:
                x = self.proj._x(lon=lo, lat=la, crs=crs_to_use)
                y = self.proj._y(lon=lo, lat=la, crs=crs_to_use)
            else:
                x, y = None, None

            ii, dxx = self._yank_inds(x, y, lo, la, crs_to_use, fast, npoints)
            inds[n] = ii
            dx[n] = dxx

        return list(itertools.chain.from_iterable(inds)), list(
            itertools.chain.from_iterable(dx)
        )

    def _yank_inds(
        self,
        x: np.ndarray,
        y: np.ndarray,
        lon: np.ndarray,
        lat: np.ndarray,
        crs_to_use: tuple[int, str],
        fast: bool,
        npoints: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Applies a cartesian or spherical search on given coordinates, finding nearest points and returning indeces and distances."""
        inds = []
        dx = []
        xlist, ylist = self.xy(crs=crs_to_use)
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
        return self._ds_manager.get_attrs().get('_global_').get('name') or 'LonelySkeleton'

    @name.setter
    def name(self, new_name: str) -> None:
        if isinstance(new_name, str):
            self.meta.append({'name': new_name})
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
                if len(empty_vars) > 3:
                    empty_vars = [empty_vars[0]] + [f'... (total of {len(empty_vars)}, use .core to view all variables)'] + [empty_vars[-1]]
                string += "\n" + "Empty variables:"
                max_len = len(max(empty_vars, key=len))
                for var in empty_vars:
                    if var[0:3] == '...':
                        string += f"\n    {var:{max_len+2}}"
                    else:
                    
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
                if len(empty_masks) > 3:
                    empty_masks = [empty_masks[0]] + [f'... (total of {len(empty_masks)}, use .core to view all masks)'] + [empty_masks[-1]]
                string += "\n" + "Empty masks:"
                max_len = len(max(empty_masks, key=len))
                for mask in empty_masks:
                    if mask[0:3] == '...':
                        string += f"\n    {mask:{max_len+2}}"
                    else:
                    
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
       

    if isinstance(coord_slice,(Iterable, int)):
        coord_slice = np.atleast_1d(coord_slice) 
        coord_inds = []
        for val in coord_slice:
            coord_inds.append(np.where(np.isclose(all_vals, val, atol=1e-8))[0])
        coord_inds = np.unique(np.concatenate(coord_inds))
        return coord_inds
    
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
