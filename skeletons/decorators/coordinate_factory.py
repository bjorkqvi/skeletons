from functools import partial
import pandas as pd
import numpy as np
from .coordinate_manager import CoordinateManager


def coord_decorator(name, grid_coord, c, stash_get=False):
    """stash_get = True means that the coordinate data can be accessed
    by method ._{name}() instead of .{name}()

    This allows for alternative definitions of the get-method elsewere."""

    def set_spacing(self, nx: int = None, dx: float = None):
        """Sets spacing for added variable"""
        z = self.get(name)

        if dx is not None:
            nx = int((max(z) - min(z)) / dx + 1)

        kwargs = {name: np.linspace(min(z), max(z), nx)}
        self._init_structure(
            self.x(strict=True),
            self.y(strict=True),
            self.lon(strict=True),
            self.lat(strict=True),
            **kwargs,
        )

    def get_coord(self, data_array=False, **kwargs):
        if not self._structure_initialized():
            return None
        data = self._ds_manager.get(name, **kwargs)
        if data_array:
            return data
        return data.values.copy()

    if not hasattr(c, "_coord_manager"):
        c._coord_manager = CoordinateManager()

    c._coord_manager.add_coord(name, grid_coord)
    if stash_get:
        exec(f"c._{name} = get_coord")
    else:
        exec(f"c.{name} = get_coord")

    exec(f"c.set_{name}_spacing = set_spacing")
    return c


def add_coord(grid_coord: bool = False, name: str = "dummy"):
    """Add a generic coordinate with no customized methods."""
    return partial(coord_decorator, name, grid_coord)


def add_time(grid_coord: bool = False, name: str = "time"):
    def wrapper(c):
        def unique_times(times, strf: str):
            return np.unique(np.array(pd.to_datetime(times).strftime(strf).to_list()))

        def hours(self, datetime=True, fmt: str = "%Y-%m-%d %H:00"):
            """Determins a Pandas data range of all the days in the time span."""
            if not self._structure_initialized():
                return None
            times = self._ds_manager.get(name).values.copy()
            if datetime:
                return pd.to_datetime(unique_times(times, "%Y-%m-%d %H"))
            else:
                return list(unique_times(times, fmt))

        def days(self, datetime=True, fmt: str = "%Y-%m-%d"):
            """Determins a Pandas data range of all the days in the time span."""
            if not self._structure_initialized():
                return None
            times = self._ds_manager.get(name).values.copy()
            if datetime:
                return pd.to_datetime(unique_times(times, "%Y-%m-%d"))
            else:
                return list(unique_times(times, fmt))

        def months(self, datetime=True, fmt: str = "%Y-%m"):
            """Determins a Pandas data range of all the months in the time span."""
            if not self._structure_initialized():
                return None
            times = self._ds_manager.get(name).values.copy()
            if datetime:
                return pd.to_datetime(unique_times(times, "%Y-%m"))
            else:
                return list(unique_times(times, fmt))

        def years(self, datetime=True, fmt: str = "%Y"):
            """Determins a Pandas data range of all the months in the time span."""
            if not self._structure_initialized():
                return None
            times = self._ds_manager.get(name).values.copy()
            if datetime:
                return pd.to_datetime(unique_times(times, "%Y"))
            else:
                return list(unique_times(times, fmt))

        def get_time(self, data_array=False, **kwargs):
            if not self._structure_initialized():
                return (None, None)
            data = self._ds_manager.get(name, **kwargs)
            if data_array:
                return data
            if len(data.values) > 1:
                return pd.to_datetime(data.values.copy())
            return pd.to_datetime([data.values[0].copy(), data.values[0].copy()])

        if not hasattr(c, "_coord_manager"):
            c._coord_manager = CoordinateManager()

        c._coord_manager.add_coord(name, grid_coord)
        exec(f"c.{name} = get_time")

        c.hours = hours
        c.days = days
        c.months = months
        c.years = years
        return c

    return wrapper


def add_frequency(grid_coord: bool = False, name: str = "freq"):
    def wrapper(c):
        def get_freq(self, angular=False):
            if not self._structure_initialized():
                return None
            freq = self._ds_manager.get(name).values.copy()
            if angular:
                freq = 2 * np.pi * freq
            return freq

        def df(self, angular=False):
            if not self._structure_initialized():
                return None
            freq = get_freq(self, angular=angular).copy()
            return (freq[-1] - freq[0]) / (len(freq) - 1)

        if not hasattr(c, "_coord_manager"):
            c._coord_manager = CoordinateManager()
        c._coord_manager.add_coord(name, grid_coord)
        exec(f"c.{name} = get_freq")
        c.df = df

        return c

    return wrapper


def add_direction(grid_coord: bool = False, name: str = "dirs"):
    def wrapper(c):
        def get_dirs(self, radians=False):
            if not self._structure_initialized():
                return None
            dirs = self._ds_manager.get(name).values.copy()
            if radians:
                dirs = dirs * np.pi / 180
            return dirs

        def ddir(self, radians=False):
            if not self._structure_initialized():
                return None
            dirs = get_dirs(self, radians=False).copy()
            dmax = 2 * np.pi if radians else 360
            return dmax / len(dirs)

        if not hasattr(c, "_coord_manager"):
            c._coord_manager = CoordinateManager()
        c._coord_manager.add_coord(name, grid_coord)
        exec(f"c.{name} = get_dirs")
        c.dd = ddir
        return c

    return wrapper
