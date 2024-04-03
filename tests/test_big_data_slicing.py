from geo_skeletons import GriddedSkeleton, PointSkeleton
from geo_skeletons.decorators import add_coord, add_time, add_datavar, add_mask

import numpy as np
import pandas as pd


# def test_get_one_time_slice():
#     @add_mask(name="land", default_value=0)
#     @add_datavar(name="dummy")
#     @add_time()
#     class Expanded(GriddedSkeleton):
#         pass

#     time = pd.date_range("2020-01-01 00:00", "2030-12-31 23:00", freq="h")
#     points = Expanded(
#         x=np.linspace(0, 100, 1000), y=np.linspace(0, 100, 1000), time=time
#     )

#     aa = points.get("dummy", empty=True)
#     # points.set("dumm3333366666633333y", points.get("dumm3333366666633333y", empty=True))
#     breakpoint()
