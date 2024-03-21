import itertools


class SkeletonIterator:
    def __init__(
        self, dict_of_coords: dict, coords_to_iterate: list[str], skeleton
    ) -> None:
        self.coords_to_iterate = coords_to_iterate
        self.dict_of_coords = dict_of_coords
        self.skeleton = skeleton
        self._compile_list()

    def __iter__(self):
        return self

    def __next__(self):
        self.ct += 1
        if self.ct < len(self.list_of_skeletons):
            return self.list_of_skeletons[self.ct]
        raise StopIteration

    def __call__(self, coords_to_iterate: list[str]):
        self.coords_to_iterate = coords_to_iterate
        self._compile_list()
        return self

    def _compile_list(self):
        coord_dict = {}
        for coord in self.coords_to_iterate:

            coord_value = self.dict_of_coords.get(coord)

            if coord_value is None:
                print(
                    f"Cannot iterate over coord {coord}, since it does not exist: {self.dict_of_coords.keys()}"
                )
            else:
                coord_dict[coord] = coord_value

        coord_tuples = itertools.product(
            *[val.values for __, val in coord_dict.items()]
        )
        list_of_skeletons = []
        for ctuple in coord_tuples:
            arg_dict = {}
            for n, val in enumerate(ctuple):
                arg_dict[list(coord_dict.keys())[n]] = val
            list_of_skeletons.append(self.skeleton.sel(arg_dict))

        self.list_of_skeletons = list_of_skeletons
        self.ct = -1
