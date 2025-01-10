from geo_skeletons import GriddedSkeleton
from geo_skeletons.decorators import (
    add_time,
    add_frequency,
    add_direction,
    add_datavar,
    add_magnitude,
)
import geo_parameters as gp
import numpy as np


@add_magnitude(name=gp.wind.Wind, x="u", y="v", direction=gp.wind.WindDir)
@add_datavar(gp.wind.YWind("u"))
@add_datavar(gp.wind.XWind("v"))
class WindGrid(GriddedSkeleton):
    pass


@add_datavar(gp.wave.Dirm, default_value=np.nan)
@add_datavar(gp.wave.Dirp, default_value=np.nan)
@add_datavar(gp.wave.Tm_10, default_value=np.nan)
@add_datavar(gp.wave.Tm02, default_value=np.nan)
@add_datavar(gp.wave.Tm01, default_value=np.nan)
@add_datavar(gp.wave.Tp, default_value=np.nan)
@add_datavar(gp.wave.Hs, default_value=np.nan)
class WaveGrid(GriddedSkeleton):
    pass
