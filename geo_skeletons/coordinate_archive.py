"""This module takes care of storing information about known coordinates and aliases"""

import geo_parameters as gp

# These are used e.g. by the coordinate_manager to keep track of mandaroty coordinates and added coordinates
SPATIAL_COORDS = ["y", "x", "lat", "lon", "inds"]


# List assumed aliases here. These are used e.g. by decoders.
TIME_ALIASES = ["time"]
X_ALIASES = ["x"]
Y_ALIASES = ["y"]
LON_ALIASES = ["lon", "longitude"]
LAT_ALIASES = ["lat", "latitude"]
FREQ_ALIASES = ["freq", "frequency"]
DIRS_ALIASES = ["dirs", "directions", "direction", "theta"]


def dict_of_coords():
    """Compiles assumed aliases to map to the known geo-parameter"""
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
