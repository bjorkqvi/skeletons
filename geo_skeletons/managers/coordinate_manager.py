from geo_parameters.metaparameter import MetaParameter
import geo_parameters as gp
import numpy as np
from geo_skeletons.errors import VariableExistsError

from geo_skeletons.variables import DataVar, Magnitude, Direction, GridMask, Coordinate
from typing import Union

from geo_skeletons.variable_archive import SPATIAL_COORDS


class CoordinateManager:
    """Keeps track of coordinates and data variables that are added to classes
    by the decorators."""

    def __init__(
        self,
        initial_coords: list[Coordinate],
        initial_vars: list[DataVar],
        metadata_manager,

    ) -> None:
        self.x_str = None
        self.y_str = None
        self._added_coords = {}
        self._added_vars = {}
        self._added_magnitudes = {}
        self._added_directions = {}
        self._added_masks = {}
        self._added_mask_points = {}

        self._list_of_initial_coords = [c.name for c in initial_coords]
        self._list_of_initial_vars = [v.name for v in initial_vars]

        self._set_initial_coords(initial_coords)
        self._set_initial_vars(initial_vars)

        self.meta = metadata_manager
        self.proj = None

    def _is_initialized(self) -> bool:
        """Check if the Dataset had been initialized"""
        return self.x_str is not None and self.y_str is not None

    def _is_altered(self) -> bool:
        """Check if the coordinate structure has been altered"""
        p1 = set(self.coords("all")) == set(self._list_of_initial_coords)
        p2 = set(self.data_vars("all")) == set(self._list_of_initial_vars)
        p3 = self._added_magnitudes == {}
        p4 = self._added_directions == {}
        p5 = self._added_masks == {}
        p6 = self._added_mask_points == {}
        return not (p1 and p2 and p3 and p4 and p5 and p6)

    def is_projected(self) -> bool:
        """Checks if the grid is projected"""
        if self.x_str == "x" and self.y_str == "y":
            return True
        elif self.x_str == "lon" and self.y_str == "lat":
            return False
        raise ValueError(
            f"Expected x- and y string to be either 'x' and 'y' or 'lon' and 'lat', but they were {self.x_str} and {self.y_str}"
        )
    
    def is_cartesian(self) -> bool:
        """Checks if the grid is natively in a cartesian projection"""
        return self.is_projected() and not self.proj.units_are_in_degrees()

    def is_rotated(self) -> bool:
        """Checks if the grid is natively in a rotated coordinates"""
        return self.is_projected () and not self.is_cartesian()

    def _add_var(self, data_var: DataVar) -> None:
        """Adds a data variable to the structure"""
        if self.get(data_var.name) is not None:
            raise VariableExistsError(data_var.name)
        self._added_vars[data_var.name] = data_var

        # Set metadata from MetaParameter if it is provided
        if data_var.meta is not None:
            self.meta.append(data_var.meta.meta_dict(), data_var.name)

    def _add_mask(self, grid_mask: GridMask) -> None:
        """Adds a mask to the structure"""
        if self.get(grid_mask.name) is not None:
            raise VariableExistsError(grid_mask.name)
        if grid_mask.triggered_by:
            grid_mask.valid_range = tuple(
                [np.inf if r is None else r for r in grid_mask.valid_range]
            )
        if isinstance(grid_mask.range_inclusive, bool):
            grid_mask.range_inclusive = (
                grid_mask.range_inclusive,
                grid_mask.range_inclusive,
            )
        self._added_masks[grid_mask.name] = grid_mask
        self._added_mask_points[grid_mask.point_name] = grid_mask

        # Set metadata from MetaParameter if it is provided
        if grid_mask.meta is not None:
            self.meta.append(grid_mask.meta.meta_dict(), grid_mask.name)

    def _triggers(self, name: str) -> list[str]:
        """Returns the masks that are triggered by a specific variable"""
        return [
            mask for mask in self._added_masks.values() if mask.triggered_by == name
        ]

    def _add_coord(self, coord: Coordinate) -> str:
        """Adds a coordinate to the structure"""
        if self.get(coord.name) is not None:
            raise VariableExistsError(coord.name)
        self._added_coords[coord.name] = coord

        # Set metadata from MetaParameter if it is provided
        if coord.meta is not None:
            self.meta.append(coord.meta.meta_dict(), coord.name)

    def _add_magnitude(self, magnitude: Magnitude) -> None:
        """Adds a magnitude to the structure"""
        if self.get(magnitude.name) is not None:
            raise VariableExistsError(magnitude.name)
        self._added_magnitudes[magnitude.name] = magnitude

        # Set metadata from MetaParameter if it is provided
        if magnitude.meta is not None:
            self.meta.append(magnitude.meta.meta_dict(), magnitude.name)

    def _add_direction(self, direction: Direction) -> None:
        """Adds a direction to the structure"""
        if self.get(direction.name) is not None:
            raise VariableExistsError(direction.name)
        self._added_directions[direction.name] = direction

        # Set metadata from MetaParameter if it is provided
        if direction.meta is not None:
            self.meta.append(direction.meta.meta_dict(), direction.name)

    def _set_initial_vars(self, initial_vars: list) -> None:
        """Set dictionary containing the initial variables of the Skeleton"""
        if not isinstance(initial_vars, list):
            raise ValueError("initial_vars needs to be a dict of DataVar's!")
        ## Class has x/y set automatically, but instance might change to lon/lat
        for var in list(self._added_vars.keys()):
            if var in SPATIAL_COORDS:
                del self._added_vars[var]
        for var in initial_vars:
            self._added_vars[var.name] = var

    def _set_initial_coords(self, initial_coords: list) -> None:
        """Set dictionary containing the initial coordinates of the Skeleton"""
        if not isinstance(initial_coords, list):
            raise ValueError("initial_coords needs to be a list of strings!")
        ## Class has x/y set automatically, but instance might change to lon/lat
        for coord in list(self._added_coords.keys()):
            if coord in SPATIAL_COORDS:
                del self._added_coords[coord]
        for coord in initial_coords:
            self._added_coords[coord.name] = coord

    def coords(self, coord_group: str = "all", cartesian: bool = None) -> list[str]:
        """Returns a list of coordinates belonging to a specific coordinate group.

        This method retrieves the coordinates that have been added to a specific group 
        (e.g., spatial, grid, gridpoint) within the Skeleton. The method can also provide coordinates
        needed to initialize a new version of a projected or spherical skeleton.

        Args:
            coord_group (str, optional): The coordinate group to retrieve. Must be one of:
                - `'all'`: Returns all added coordinates.
                - `'spatial'`: Returns spatial coordinates (e.g., `inds`, `lat`, `lon`, `x`, `y`).
                - `'nonspatial'`: Returns all coordinates except spatial ones (e.g., `time`, `freq`, `dirs`).
                - `'init'`: Returns the coordinates required for initializing the Skeleton, 
                including all non-spatial coordinates, and spatial coordinates (`lat/lon`, `x/y`), 
                but excluding `'inds'`.
                    * If `cartesian=True`, only `x/y` coordinates are returned.
                    * If `cartesian=False`, only `lon/lat` coordinates are returned.
                - `'grid'`: Returns grid coordinates (e.g., `z`, `time`) along with spatial coordinates.
                - `'gridpoint'`: Returns grid point coordinates (e.g., `freq`, `dirs`).
            cartesian (bool, optional): When `coord_group='init'`, this specifies whether to filter 
                for cartesian (`x/y`) or spherical (`lon/lat`) coordinates:
                - `True`: Returns only cartesian coordinates (`x`, `y`).
                - `False`: Returns only spherical coordinates (`lon`, `lat`).
                - If `None`, includes both. Defaults to None.

        Returns:
            list[str]: A list of coordinate names belonging to the specified group.

        Examples:
            Create a GriddedSkeleton with various coordinate groups:
            >>> Spectrum = GriddedSkeleton.add_time().add_frequency().add_direction()
            >>> Spectrum.core
            ------------------------------ Coordinate groups -------------------------------
            Spatial:    (y, x)
            Grid:       (time, y, x)
            Gridpoint:  (freq, dirs)
            All:        (time, y, x, freq, dirs)

            Get spatial coordinates:
            >>> Spectrum.core.coords('spatial')
            ['y', 'x']

            Get grid point coordinates:
            >>> Spectrum.core.coords('gridpoint')
            ['freq', 'dirs']

            Get grid coordinates (including spatial):
            >>> Spectrum.core.coords('grid')
            ['time', 'y', 'x']

            Get non-spatial coordinates:
            >>> Spectrum.core.coords('nonspatial')
            ['time', 'freq', 'dirs']

            Get all coordinates:
            >>> Spectrum.core.coords('all')
            ['time', 'y', 'x', 'freq', 'dirs']

            Get initialization coordinates (both cartesian and spherical):
            >>> Spectrum.core.coords('init')
            ['time', 'lat', 'lon', 'y', 'x', 'dirs', 'freq']

            Get initialization coordinates filtered for cartesian:
            >>> Spectrum.core.coords('init', cartesian=True)
            ['time', 'y', 'x', 'dirs', 'freq']

            Get initialization coordinates filtered for spherical:
            >>> Spectrum.core.coords('init', cartesian=False)
            ['time', 'lat', 'lon', 'dirs', 'freq']


            Create a PointSkeleton with various coordinate groups:
            >>> PointSpectrum = PointSkeleton.add_time().add_frequency().add_direction()
            >>> PointSpectrum.core
            ------------------------------ Coordinate groups -------------------------------
            Spatial:    (inds)
            Grid:       (time, inds)
            Gridpoint:  (freq, dirs)
            All:        (time, inds, freq, dirs)

            Get all spatial coordinates
            >>> PointSpectrum.core.coords('spatial')
            ['inds']
            
            Get all coordinates needed to initialize skeleton (don via 'lon'/'lat', not 'inds')
            >>> PointSpectrum.core.coords('init', cartesian=False)
            ['time', 'lat', 'lon', 'dirs', 'freq']
        """
        if coord_group not in [
            "all",
            "spatial",
            "nonspatial",
            "grid",
            "gridpoint",
            "init",
        ]:
            raise ValueError(
                "Coord group needs to be 'all', 'spatial', 'nonspatial', 'grid', 'gridpoint' or 'init'."
            )

        if coord_group == "all":
            coords = self._added_coords.values()
        elif coord_group == "nonspatial":
            coords = [
                coord
                for coord in self._added_coords.values()
                if coord.coord_group != "spatial"
            ]
        elif coord_group == "grid":
            coords = [
                coord
                for coord in self._added_coords.values()
                if coord.coord_group in [coord_group, "spatial"]
            ]
        elif coord_group == "init":
            coords = list(set(self.coords()) - set(["inds"])) + self.data_vars(
                "spatial"
            )
            if (
                not self._is_initialized()
            ):  # Can use either x/y or lon/lat if it has not yet been determined
                coords = coords + ["lon", "lat"]
            if cartesian is not None:
                if cartesian:
                    coords = list(set(coords) - {"lon", "lat"})
                else:
                    coords = list(set(coords) - {"x", "y"})
        else:
            coords = [
                coord
                for coord in self._added_coords.values()
                if coord.coord_group == coord_group
            ]

        if coord_group != "init":
            coords = [coord.name for coord in coords]

        return move_time_and_spatial_to_front(coords)

    def masks(self, coord_group: str = "all") -> list[str]:
        """Returns a list of masks added to a specific coordinate group.

        This method retrieves the names of masks that have been added to the Skeleton 
        and are associated with the specified coordinate group.

        Args:
            coord_group (str, optional): The coordinate group to retrieve masks for. Must be one of:
                - `'all'`: Returns all added masks (default behaviour).
                - `'spatial'`: Returns masks associated with spatial coordinates (e.g., `inds`, `lat/lon`).
                - `'nonspatial'`: Returns masks not associated with spatial coordinates.
                - `'grid'`: Returns masks associated with grid coordinates (e.g., `z`, `time`) and spatial coordinates.
                - `'gridpoint'`: Returns masks associated with grid points (e.g., `frequency`, `direction`).

        Returns:
            list[str]: A list of mask names in the specified coordinate group.

        Notes:
            - Query for coord_group 'grid' accepts also purely spatial variables

        Examples:
            Create a PointSkeleton with masks:
            >>> PointSpectrum = PointSkeleton.add_time().add_frequency().add_direction()
                .add_mask('land', coord_group='spatial')
                .add_mask('swell', coord_group='gridpoint')
                .add_mask('missing', coord_group='grid')

            Inspect the coordinate groups and masks:
            >>> PointSpectrum.core
            ------------------------------ Coordinate groups -------------------------------
            Spatial:    (inds)
            Grid:       (time, inds)
            Gridpoint:  (freq, dirs)
            All:        (time, inds, freq, dirs)
            ------------------------------------- Data -------------------------------------
            Variables:
                *empty*
            Masks:
                land_mask     (inds):  False
                swell_mask    (freq, dirs):  False
                missing_mask  (time, inds):  False
            Magnitudes:
                *empty*
            Directions:
                *empty*
            --------------------------------------------------------------------------------

            Get all masks:
            >>> PointSpectrum.core.masks('all')
            ['land_mask', 'swell_mask', 'missing_mask']

            Get spatial masks:
            >>> PointSpectrum.core.masks('spatial')
            ['land_mask']

            Get grid masks:
            >>> PointSpectrum.core.masks('grid')
            ['land_mask','missing_mask']

            Get gridpoint masks:
            >>> PointSpectrum.core.masks('gridpoint')
            ['swell_mask']
        """
        if coord_group not in ["all", "spatial", "nonspatial", "grid", "gridpoint"]:
            print(
                "Coord group needs to be 'all', 'spatial', 'nonspatial','grid' or 'gridpoint'."
            )
            return None

        if coord_group == "all":
            masks = self._added_masks.values()
        elif coord_group == "nonspatial":
            masks = [
                mask
                for mask in self._added_masks.values()
                if mask.coord_group != "spatial"
            ]
        elif coord_group == "grid":
            masks = [
                mask
                for mask in self._added_masks.values()
                if mask.coord_group in [coord_group, "spatial"]
            ]
        else:
            masks = [
                mask
                for mask in self._added_masks.values()
                if mask.coord_group == coord_group
            ]

        return [mask.name for mask in masks]

    def mask_points(self, coord_group: str = "all") -> list[str]:
        """Returns a list of mask points added to a specific coordinate group.

        This method retrieves the names of mask points that have been added to the Skeleton 
        and are associated with the specified coordinate group.

        Args:
            coord_group (str, optional): The coordinate group to retrieve mask points for. Must be one of:
                - `'all'`: Returns all added mask points (default behaviour).
                - `'spatial'`: Returns mask points associated with spatial coordinates (e.g., `inds`, `lat/lon`).
                - `'nonspatial'`: Returns mask points not associated with spatial coordinates.
                - `'grid'`: Returns mask points associated with grid coordinates (e.g., `z`, `time`) and spatial coordinates.
                - `'gridpoint'`: Returns mask points associated with grid points (e.g., `frequency`, `direction`).

        Returns:
            list[str]: A list of mask point names in the specified coordinate group.

        Notes:
            - Query for coord_group 'grid' accepts also purely spatial variables

        Examples:
            Create a PointSkeleton with mask points:
            >>> PointSpectrum = PointSkeleton.add_time().add_frequency().add_direction()
                .add_mask('land', coord_group='spatial')
                .add_mask('swell', coord_group='gridpoint')
                .add_mask('missing', coord_group='grid')

            Inspect the coordinate groups and mask points:
            >>> PointSpectrum.core
            ------------------------------ Coordinate groups -------------------------------
            Spatial:    (inds)
            Grid:       (time, inds)
            Gridpoint:  (freq, dirs)
            All:        (time, inds, freq, dirs)
            ------------------------------------- Data -------------------------------------
            Variables:
                *empty*
            Masks:
                land_mask     (inds):  False
                swell_mask    (freq, dirs):  False
                missing_mask  (time, inds):  False
            Magnitudes:
                *empty*
            Directions:
                *empty*
            --------------------------------------------------------------------------------

            Get all mask points:
            >>> PointSpectrum.core.mask_points('all')
            ['land_points', 'swell_points', 'missing_points']

            Get spatial mask points:
            >>> PointSpectrum.core.mask_points('spatial')
            ['land_points']

            Get grid mask points:
            >>> PointSpectrum.core.mask_points('grid')
            ['land_points','missing_points']

            Get gridpoint mask points:
            >>> PointSpectrum.core.mask_points('gridpoint')
            ['swell_points']
        """
        if coord_group not in ["all", "spatial", "nonspatial", "grid", "gridpoint"]:
            print(
                "Coord group needs to be 'all', 'spatial', 'nonspatial','grid' or 'gridpoint'."
            )
            return None

        if coord_group == "all":
            masks = self._added_mask_points.values()
        elif coord_group == "nonspatial":
            masks = [
                mask
                for mask in self._added_mask_points.values()
                if mask.coord_group != "spatial"
            ]
        elif coord_group == "grid":
            masks = [
                mask
                for mask in self._added_mask_points.values()
                if mask.coord_group in [coord_group, "spatial"]
            ]
        else:
            masks = [
                mask
                for mask in self._added_mask_points.values()
                if mask.coord_group == coord_group
            ]

        return [mask.point_name for mask in masks]

    def _mask_is_primary(self, name: str) -> bool:
        """Checks if a mask is a primary mask or an mask defined as an opposite to a primary mask"""
        mask = self._added_masks.get(f"{name}")
        if mask is None:
            raise ValueError(f"No mask: '{name}'!")
        return mask.primary_mask

    def _find_primary_mask(self, name: str) -> bool:
        """Finds the name of the primary mask to a given opposite_mask"""
        masks = self.masks()
        for mask in masks:
            if self._added_masks.get(mask).opposite_mask is not None:
                if self._added_masks.get(mask).opposite_mask.name == name:
                    return mask
        return None

    def data_vars(self, coord_group: str = "nonspatial") -> list[str]:
        """Returns a list of data variables added to a specific coordinate group.

        This method retrieves the names of data variables that have been added to the Skeleton 
        and are associated with a specified coordinate group. If no `coord_group` is provided, 
        it returns all added data variables.

        Args:
            coord_group (Optional[str], optional): The coordinate group to retrieve data variables for. 
                Must be one of:
                - `all`: Returns all added data variables.
                - `'spatial'`: Returns data variables associated with spatial coordinates (e.g., `inds`, `x`, `y`).
                - `'nonspatial'`: Returns data variables not associated with spatial coordinates (default behaviour).
                - `'grid'`: Returns data variables associated with grid coordinates (e.g., `time`, `z`) and spatial coordinates.
                - `'gridpoint'`: Returns data variables associated with grid point coordinates 
                (e.g., `frequency`, `direction`).

        Returns:
            list[str]: A list of data variable names in the specified coordinate group.

        Raises:
            ValueError: If an invalid value is provided for `coord_group`.

        Notes:
            - Query for coord_group 'grid' accepts also purely spatial variables
            - For `PointSkeleton`, spatial data variables such as `x` and `y` are included in the `'spatial'` group.
            - For `GriddedSkeleton`, spatial data variables are tied to grid coordinates (e.g., `y`, `x`).

        Examples:
            Example with a GriddedSkeleton:
            >>> Wave = GriddedSkeleton.add_time()
                .add_datavar('hs', coord_group='grid')
                .add_datavar('tp')
                .add_datavar('depth', coord_group='spatial')

            Inspect the coordinate groups and data variables:
            >>> Wave.core
            ------------------------------ Coordinate groups -------------------------------
            Spatial:    (y, x)
            Grid:       (time, y, x)
            Gridpoint:  *empty*
            All:        (time, y, x)
            ------------------------------------- Data -------------------------------------
            Variables:
                hs     (time, y, x):  0.0
                tp     (time, y, x):  0.0
                depth  (y, x):  0.0
            Masks:
                *empty*
            Magnitudes:
                *empty*
            Directions:
                *empty*
            --------------------------------------------------------------------------------

            Get all data variables:
            >>> Wave.core.data_vars()
            ['hs', 'tp']

            Get spatial data variables:
            >>> Wave.core.data_vars('spatial')
            ['depth']

            Get grid data variables:
            >>> Wave.core.data_vars('grid')
            ['hs', 'depth']

            Get gridpoint data variables:
            >>> Wave.core.data_vars('gridpoint')
            []

            Example with a PointSkeleton:
            >>> Wave = PointSkeleton.add_time()
                .add_datavar('hs', coord_group='grid')
                .add_datavar('tp')
                .add_datavar('depth', coord_group='spatial')

            Inspect the coordinate groups and data variables:
            >>> Wave.core
            ------------------------------ Coordinate groups -------------------------------
            Spatial:    (inds)
            Grid:       (time, inds)
            Gridpoint:  *empty*
            All:        (time, inds)
            ------------------------------------- Data -------------------------------------
            Variables:
                y      (inds):  0 [m] distance_in_y_direction
                x      (inds):  0 [m] distance_in_x_direction
                hs     (time, inds):  0.0
                tp     (time, inds):  0.0
                depth  (inds):  0.0
            Masks:
                *empty*
            Magnitudes:
                *empty*
            Directions:
                *empty*
            --------------------------------------------------------------------------------

            Get all data variables:
            >>> Wave.core.data_vars()
            ['hs', 'tp']

            Get spatial data variables:
            >>> Wave.core.data_vars('spatial')
            ['depth', 'x', 'y']

            Get grid data variables:
            >>> Wave.core.data_vars('grid')
            ['hs', 'depth','x','y']

            Get gridpoint data variables:
            >>> Wave.core.data_vars('gridpoint')
            []
            
        
        """
        if coord_group not in ["all", "spatial", "nonspatial", "grid", "gridpoint"]:
            print(
                "Coord group needs to be 'all', 'spatial', 'nonspatial','grid' or 'gridpoint'."
            )
            return None

        if coord_group == "all":
            vars = self._added_vars.values()
        elif coord_group == "nonspatial":
            vars = [
                var for var in self._added_vars.values() if var.coord_group != "spatial"
            ]
        elif coord_group == "grid":
            vars = [
                var
                for var in self._added_vars.values()
                if var.coord_group in [coord_group, "spatial"]
            ]
        else:
            vars = [
                var
                for var in self._added_vars.values()
                if var.coord_group == coord_group
            ]

        return move_time_and_spatial_to_front([var.name for var in vars if var.name])

    def magnitudes(self, coord_group: str = "all") -> list[str]:
        """Returns a list of magnitudes that have been added to a specific coordinate group.

        This method retrieves the names of magnitude variables associated with a specified 
        coordinate group. If no `coord_group` is provided, it returns all added magnitudes.

        Args:
            coord_group (Optional[str], optional): The coordinate group to retrieve magnitudes for. 
                Must be one of:
                - `all`: Returns all added magnitudes (default behavior).
                - `'spatial'`: Returns magnitudes associated with spatial coordinates (e.g., `inds`, `lat/lon`).
                - `'nonspatial'`: Returns magnitudes not associated with spatial coordinates (e.g., `time`, `frequency`).
                - `'grid'`: Returns magnitudes associated with grid coordinates (e.g., `time`, `z`) and spatial coordinates.
                - `'gridpoint'`: Returns magnitudes associated with grid point coordinates 
                (e.g., `frequency`, `direction`).

        Returns:
            list[str]: A list of magnitude variable names in the specified coordinate group.

        Raises:
            ValueError: If the provided `coord_group` is not one of the valid options.

        Notes:
            - If `coord_group=None`, all magnitudes are returned regardless of their association.
            - Magnitudes are variables that represent scalar quantities, such as wave heights 
            or intensities, tied to their respective coordinates.
        """
        if coord_group not in ["all", "spatial", "nonspatial", "grid", "gridpoint"]:
            print(
                "Coord group needs to be 'all', 'spatial', 'nonspatial','grid' or 'gridpoint'."
            )
            return None

        if coord_group == "all":
            vars = self._added_magnitudes.values()
        elif coord_group == "nonspatial":
            vars = [
                var
                for var in self._added_magnitudes.values()
                if var.x.coord_group != "spatial"
            ]
        elif coord_group == "grid":
            vars = [
                var
                for var in self._added_magnitudes.values()
                if var.x.coord_group in [coord_group, "spatial"]
            ]
        else:
            vars = [
                var
                for var in self._added_magnitudes.values()
                if var.x.coord_group == coord_group
            ]

        return [var.name for var in vars]

    def directions(self, coord_group: str = "all") -> list[str]:
        """Returns a list of directions that have been added to a specific coordinate group.

        This method retrieves the names of direction variables associated with a specified 
        coordinate group. If no `coord_group` is provided, it returns all added directions.

        Args:
            coord_group (Optional[str], optional): The coordinate group to retrieve directions for. 
                Must be one of:
                - `all`: Returns all added directions (default behavior).
                - `'spatial'`: Returns directions associated with spatial coordinates (e.g., `inds`, `lat/lon`).
                - `'nonspatial'`: Returns directions not associated with spatial coordinates (e.g., `time`, `frequency`).
                - `'grid'`: Returns directions associated with grid coordinates (e.g., `time`, `z`) and spatial coordinates.
                - `'gridpoint'`: Returns directions associated with grid point coordinates 
                (e.g., `frequency`, `direction`).

        Returns:
            list[str]: A list of direction variable names in the specified coordinate group.

        Raises:
            ValueError: If the provided `coord_group` is not one of the valid options.

        Notes:
            - If `coord_group=None`, all directions are returned regardless of their association.
            - Directions are variables that represent angular quantities or orientations 
            tied to their respective coordinates, such as wind or wave directions.
        """
        if coord_group not in ["all", "spatial", "nonspatial", "grid", "gridpoint"]:
            print(
                "Coord group needs to be 'all', 'spatial', 'nonspatial','grid' or 'gridpoint'."
            )
            return None

        if coord_group == "all":
            vars = self._added_directions.values()
        elif coord_group == "nonspatial":
            vars = [
                var
                for var in self._added_directions.values()
                if var.x.coord_group != "spatial"
            ]
        elif coord_group == "grid":
            vars = [
                var
                for var in self._added_directions.values()
                if var.x.coord_group in [coord_group, "spatial"]
            ]
        else:
            vars = [
                var
                for var in self._added_directions.values()
                if var.x.coord_group == coord_group
            ]

        return [var.name for var in vars]

    def all_objects(self, coord_group: str = "all") -> list[str]:
        """Returns a list of all objects in the specified coordinate group.

        This method retrieves the names of all objects (e.g., data variables, coordinates, 
        magnitudes, directions, masks) associated with the specified coordinate group.

        Args:
            coord_group (str, optional): The coordinate group to retrieve objects for. 
                Must be one of:
                - `'all'`: Returns all objects (default behaviour).
                - `'spatial'`: Returns objects associated with spatial coordinates.
                - `'nonspatial'`: Returns objects not associated with spatial coordinates.
                - `'grid'`: Returns objects associated with grid coordinates.
                - `'gridpoint'`: Returns objects associated with grid points.

        Returns:
            list[str]: A list of names of all objects in the specified coordinate group.
        """
        list_of_objects = (
            self.data_vars(coord_group)
            + self.coords(coord_group)
            + self.magnitudes(coord_group)
            + self.directions(coord_group)
            + self.masks(coord_group)
        )
        return list_of_objects

    def non_coord_objects(self, coord_group: str = "all") -> list[str]:
        """Returns a list of all non-coordinate objects in the specified coordinate group.

        This method retrieves the names of all objects in the given coordinate group, excluding 
        coordinates and spatial data variables (e.g., `x` and `y` in a `PointSkeleton`).

        Args:
            coord_group (str, optional): The coordinate group to retrieve objects for. 
                Must be one of:
                - `'all'`: Returns all non-coordinate objects (default behaviour).
                - `'spatial'`: Returns non-coordinate objects associated with spatial coordinates.
                - `'nonspatial'`: Returns non-coordinate objects not associated with spatial coordinates.
                - `'grid'`: Returns non-coordinate objects associated with grid coordinates.
                - `'gridpoint'`: Returns non-coordinate objects associated with grid points.

        Returns:
            list[str]: A list of names of all non-coordinate objects in the specified coordinate group.
        """
        not_accepted = set(self.coords("all") + self.data_vars("spatial"))
        all_objects = set(self.all_objects(coord_group))
        accepted = all_objects - not_accepted
        return list(accepted)

    def coord_group(self, var: str) -> str:
        """Returns the coordinate group that a variable or mask is defined over.

        This method identifies the coordinate group (e.g., `'spatial'`, `'grid'`, etc.) 
        associated with a given variable, mask, magnitude, or direction.

        Args:
            var (str): The name of the variable or mask to check.

        Returns:
            str: The coordinate group the variable or mask is associated with.

        Raises:
            KeyError: If the variable or mask is not found.

        Notes:
            - The coordinates for the returned group can be retrieved using the `.coords()` method.
        """
        coords = [v for v in self._added_coords.values() if v.name == var]
        vars = [v for v in self._added_vars.values() if v.name == var]
        masks = [v for v in self._added_masks.values() if v.name == var]
        mags = [v for v in self._added_magnitudes.values() if v.name == var]
        dirs = [v for v in self._added_directions.values() if v.name == var]
        all_vars = coords + vars + masks + mags + dirs
        if not all_vars:
            raise KeyError(f"Cannot get coord_group for unknown variable {var}!")

        return all_vars[0].coord_group

    def get(
        self, var: str
    ) -> Union[Coordinate, DataVar, Magnitude, Direction, GridMask, None]:
        """Returns the object associated with a given name.

        This method retrieves a coordinate, data variable, magnitude, direction, 
        or mask by its name.

        Args:
            var (str): The name of the object to retrieve.

        Returns:
            Union[Coordinate, DataVar, Magnitude, Direction, GridMask, None]: The object 
            associated with the given name, or `None` if not found.
        """
        return (
            self._added_coords.get(var)
            or self._added_vars.get(var)
            or self._added_magnitudes.get(var)
            or self._added_directions.get(var)
            or self._added_masks.get(var)
        )

    def meta_parameter(self, var: str) -> Union[MetaParameter, None]:
        """Returns the meta parameter for a given variable.

        This method retrieves the meta parameter associated with a given variable, 
        if available.

        Args:
            var (str): The name of the variable to retrieve the meta parameter for.

        Returns:
            Union[MetaParameter, None]: The meta parameter associated with the variable, 
            or `None` if no meta parameter is available.
        """
        param = self.get(var)
        if param is None:
            return None
        return param.meta

    def default_value(self, var: str) -> Union[int, float, None]:
        """Returns the default value for a given parameter.

        This method retrieves the default value associated with a given parameter, 
        if available.

        Args:
            var (str): The name of the parameter to retrieve the default value for.

        Returns:
            Union[int, float, None]: The default value of the parameter, or `None` if 
            no default value is set or the parameter does not exist.
        """
        param = self.get(var)
        if param is None:
            return None
        if not hasattr(param, "default_value"):
            return None
        return param.default_value

    def get_dir_type(self, name: str) -> Union[str, None]:
        """Gets the `dir_type` of a variable, if applicable.

        This method retrieves the `dir_type` (e.g., `'from'`, `'to'`, or `'math'`) 
        of a variable, if the variable has a directional type.

        Args:
            name (str): The name of the variable to retrieve the `dir_type` for.

        Returns:
            Union[str, None]: The `dir_type` of the variable, or `None` if the variable 
            does not have a `dir_type`.
        """
        obj = self.get(name)
        if obj is None:
            return None
        if not hasattr(obj, "dir_type"):
            return None
        return obj.dir_type

    def find_cf(self, standard_name: str) -> list[str]:
        """Finds variable names with a given CF standard name.

        This method searches for variables that have the specified CF (Climate and 
        Forecast) standard name or its alias.

        Args:
            standard_name (str): The CF standard name to search for.

        Returns:
            list[str]: A list of variable names that have the specified standard name.
        """
        names = []

        for name in self.all_objects():
            obj = self.get(name)
            if obj.meta is None:
                continue
            if (
                obj.meta.standard_name() == standard_name
                or obj.meta.standard_name(alias=True) == standard_name
            ):
                names.append(obj.name)

        return names

    def find(self, param: Union[MetaParameter, str]) -> list[str]:
        """Finds the names of parameters based on a CF standard name or MetaParameter.

        This method searches for parameters in the Skeleton that match a given CF 
        standard name or `MetaParameter`. If multiple matches exist and a `MetaParameter` 
        is provided, a name match is attempted.

        Args:
            param (Union[MetaParameter, str]): The standard name or `MetaParameter` to search for.

        Returns:
            list[str]: A list of parameter names that match the given standard name or `MetaParameter`.
        """
        if gp.is_gp(param):
            std_name = param.standard_name()
        else:
            std_name = param

        names = self.find_cf(std_name)

        if len(names) < 2 or not gp.is_gp_instance(param):
            return names
        else:
            clean_names = [n for n in names if n == param.name]
            if clean_names:
                return clean_names
            return names

    def find_twin_component(self, param: Union[MetaParameter, str]) -> Union[str, None]:
        """Finds the twin component of a given parameter.

        This method identifies the complementary component (e.g., `u` for `v` and vice versa) 
        of a specified parameter.

        Args:
            param (Union[MetaParameter, str]): The parameter or its name to find the twin component for.

        Returns:
            Union[str, None]: The name of the twin component, or `None` if no twin component is found.

        Raises:
            ValueError: If multiple candidates for the twin component are found.
        """
        if isinstance(param, str):
            param = self.meta_parameter(param)
        
        if param is None:
            return None

        my_type = param.i_am()
        if my_type == 'x':
            twin = self.find(param.my_family('y'))
        elif my_type == 'y':
            twin = self.find(param.my_family('x'))
        else:
            return None

        if len(twin) > 1:
            raise ValueError(f"Found two possible candidates for a second component: {twin}")
        
        
        if not twin:
            return None
        
        return twin[0]
    
    def __repr__(self):
        def string_of_coords(list_of_coords) -> str:
            if not list_of_coords:
                return ""
            string = "("
            for c in list_of_coords:
                string += f"{c}, "
            string = string[:-2]
            string += ")"
            return string

        string = f"{' Coordinate groups ':-^80}" + "\n"
        string += f"{'Spatial:':12}"

        string += string_of_coords(self.coords("spatial")) or "*empty*"
        string += f"\n{'Grid:':12}"
        string += string_of_coords(self.coords("grid")) or "*empty*"
        string += f"\n{'Gridpoint:':12}"
        string += string_of_coords(self.coords("gridpoint")) or "*empty*"

        string += f"\n{'All:':12}"
        string += string_of_coords(self.coords("all")) or "*empty*"

        string += f"\n{' Data ':-^80}"
        string += "\n" + "Variables:"
        if self.data_vars():
            max_len = len(max(self.data_vars(), key=len))
            for var in self.data_vars():
                string += f"\n    {var:{max_len+2}}"
                string += string_of_coords(self.coords(self.coord_group(var)))
                string += f":  {self.default_value(var)}"
                meta_parameter = self.meta_parameter(var)
                if meta_parameter is not None:
                    string += f" [{meta_parameter.units()}]"
                    string += f" {meta_parameter.standard_name()}"
        else:
            string += "\n    *empty*"

        string += "\n" + "Masks:"
        if self.masks():
            max_len = len(max(self.masks(), key=len))
            for mask in self.masks():
                string += f"\n    {mask:{max_len+2}}"
                string += string_of_coords(self.coords(self.coord_group(mask)))
                string += f":  {bool(self.default_value(mask))}"
        else:
            string += "\n    *empty*"

        magnitudes = self.magnitudes()
        string += "\n" + "Magnitudes:"
        if magnitudes:

            for key in magnitudes:
                value = self.get(key)
                string += f"\n  {key}: magnitude of ({value.x},{value.y})"

                meta_parameter = self.meta_parameter(key)
                if meta_parameter is not None:
                    string += f" [{meta_parameter.units()}]"
                    string += f" {meta_parameter.standard_name()}"
        else:
            string += "\n    *empty*"
        directions = self.directions()
        string += "\n" + "Directions:"
        if directions:

            for key in directions:
                value = self.get(key)
                string += f"\n  {key}: direction of ({value.x},{value.y})"
                meta_parameter = self.meta_parameter(key)
                if meta_parameter is not None:
                    string += f" [{meta_parameter.units()}]"
                    string += f" {meta_parameter.standard_name()}"
        else:
            string += "\n    *empty*"

        string += "\n" + "-" * 80
        return string


def move_time_and_spatial_to_front(coord_list: list[str]) -> list[str]:
    """Makes sure that the coordinate list starts with 'time', followed by the spatial coords"""
    if "inds" in coord_list:
        coord_list.insert(0, coord_list.pop(coord_list.index("inds")))
    if "x" in coord_list:
        coord_list.insert(0, coord_list.pop(coord_list.index("x")))
    if "y" in coord_list:
        coord_list.insert(0, coord_list.pop(coord_list.index("y")))
    if "lon" in coord_list:
        coord_list.insert(0, coord_list.pop(coord_list.index("lon")))
    if "lat" in coord_list:
        coord_list.insert(0, coord_list.pop(coord_list.index("lat")))
    if "time" in coord_list:
        coord_list.insert(0, coord_list.pop(coord_list.index("time")))
    return coord_list
