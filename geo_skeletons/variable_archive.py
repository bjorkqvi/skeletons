"""This module takes care of storing information about known coordinates and aliases"""

import geo_parameters as gp
from typing import Union
from geo_parameters.metaparameter import MetaParameter

# These are used e.g. by the coordinate_manager to keep track of mandaroty coordinates and added coordinates
SPATIAL_COORDS = ["y", "x", "lat", "lon", "inds"]


# List assumed coordinate aliases here. These are used e.g. by decoders.
TIME_ALIASES = ["time"]
X_ALIASES = ["x"]
Y_ALIASES = ["y"]
LON_ALIASES = ["lon", "longitude"]
LAT_ALIASES = ["lat", "latitude"]
FREQ_ALIASES = ["freq", "frequency"]
DIRS_ALIASES = ["dirs", "directions", "direction", "theta"]

# List assumed data variable aliases here
HS_ALIASES = ["hs", "hsig", "swh", "hm0", "vhm0"]
TP_ALIASES = ["tp"]
WIND_ALIASES = ["ff", "wind", "wind_speed", "windspeed"]
WINDDIR_ALIASES = ["dd", "windir", "wind_dir", "winddir", "wind_direction"]
XWIND_ALIASES = ["x_wind", "xwnd"]
YWIND_ALIASES = ["y_wind", "ywnd"]
XCURRENT_ALIASES = ["x_current", "xcur"]
YCURRENT_ALIASES = ["y_current", "ycur"]
DEPTH_ALIASES = ["depth", "water_depth"]


def coord_alias_map_to_gp() -> dict[str, Union[MetaParameter, str]]:
    """Compiles assumed aliases of coordinates to map to the known geo-parameter"""
    coord_dict = {}
    for var in LON_ALIASES:
        coord_dict[var] = gp.grid.Lon
    for var in LAT_ALIASES:
        coord_dict[var] = gp.grid.Lat
    for var in X_ALIASES:
        coord_dict[var] = gp.grid.X
    for var in Y_ALIASES:
        coord_dict[var] = gp.grid.Y
    for var in FREQ_ALIASES:
        coord_dict[var] = gp.wave.Freq
    for var in DIRS_ALIASES:
        coord_dict[var] = gp.wave.Dirs
    for var in TIME_ALIASES:
        coord_dict[var] = "time"

    return coord_dict


def var_alias_map_to_gp() -> dict[str, Union[MetaParameter, str]]:
    """Compiles assumed aliases of variables to map to the known geo-parameter"""
    var_dict = {}
    for var in HS_ALIASES:
        var_dict[var] = gp.wave.Hs
    for var in TP_ALIASES:
        var_dict[var] = gp.wave.Tp
    for var in WIND_ALIASES:
        var_dict[var] = gp.wind.Wind
    for var in WINDDIR_ALIASES:
        var_dict[var] = gp.wind.WindDir
    for var in XWIND_ALIASES:
        var_dict[var] = gp.wind.XWind
    for var in YWIND_ALIASES:
        var_dict[var] = gp.wind.YWind
    for var in XCURRENT_ALIASES:
        var_dict[var] = gp.ocean.XCurrent
    for var in YCURRENT_ALIASES:
        var_dict[var] = gp.ocean.YCurrent
    for var in DEPTH_ALIASES:
        var_dict[var] = gp.ocean.WaterDepth
    return var_dict
