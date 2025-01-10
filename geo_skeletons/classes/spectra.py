from geo_skeletons import PointSkeleton
from geo_skeletons.decorators import add_time, add_frequency, add_direction, add_datavar
import geo_parameters as gp


@add_datavar(gp.wave.Ef)
@add_frequency()
@add_time()
class Spectrum1D(PointSkeleton):
    pass


@add_datavar(gp.wave.Efth)
@add_direction()
@add_frequency()
@add_time()
class Spectrum2D(PointSkeleton):
    pass
