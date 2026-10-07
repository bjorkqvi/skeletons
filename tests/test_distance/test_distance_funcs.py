
import numpy as np
from geo_skeletons.distance_funcs import get_dist_point, get_dist_point_approx_pruned

# Generate 1 million random points
import time

def test_dist_func_speed():
    np.random.seed(42)  # For reproducibility
    num_points = 1_000_000
    x = np.random.uniform(0, 1000, num_points)
    y = np.random.uniform(0, 1000, num_points)

    # Measure execution time for unpruned version
    start_time = time.time()
    unpruned_distances = get_dist_point(x, y)
    unpruned_time = time.time() - start_time
    print(f"Unpruned version took {unpruned_time:.2f} seconds.")

    # Measure execution time for pruned version
    start_time = time.time()
    pruned_distances = get_dist_point_approx_pruned(x, y, threshold=2)
    pruned_time = time.time() - start_time
    print(f"Pruned version took {pruned_time:.2f} seconds.")

    # Validate that both versions produce similar results
    # Since pruning uses a heuristic, some small differences may exist
    if np.allclose(unpruned_distances, pruned_distances, atol=1e-5):
        print("Both versions produce similar results.")
    else:
        print("The results differ between the two versions.")