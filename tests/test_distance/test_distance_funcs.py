
import numpy as np
from geo_skeletons.distance_funcs import get_dist_point

# Generate 1 million random points
import time

def test_dist_func_speed():
    np.random.seed(42)  # For reproducibility
    num_points = 1_000
    x = np.random.uniform(0, 100000, num_points)
    y = np.random.uniform(0, 100000, num_points)

    # Measure execution time for unpruned version
    start_time = time.time()
    unpruned_distances = get_dist_point(tuple(x), tuple(y))
    unpruned_time = time.time() - start_time
    print(f"Unpruned version took {unpruned_time:.2f} seconds.")

