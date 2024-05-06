from __future__ import annotations
from typing import TYPE_CHECKING
import itertools

if TYPE_CHECKING:
    from .skeleton import Skeleton


class SkeletonIterator:
    def __init__(
        self, dict_of_coords: dict, coords_to_iterate: list[str], skeleton
    ) -> None:

        self.dict_of_coords = dict_of_coords
        self.coords_to_iterate = list(coords_to_iterate)
        # Needed to get the ordering right with itertools
        self.coords_to_iterate.reverse()
        self.skeleton = skeleton
        self.list_of_skeletons = self._compile_list()
        self.ct = -1

    def __iter__(self) -> SkeletonIterator:
        return self

    def __next__(self) -> Skeleton:
        self.ct += 1
        if self.ct < len(self.list_of_skeletons):
            return self.list_of_skeletons[self.ct]
        raise StopIteration

    def __call__(self, coords_to_iterate: list[str]) -> SkeletonIterator:
        self.coords_to_iterate = list(coords_to_iterate)
        # Needed to get the ordering right with itertools
        self.coords_to_iterate.reverse()
        self.list_of_skeletons = self._compile_list()
        self.ct = -1
        return self

    def _compile_list(self) -> list[Skeleton]:
        """Returns a list of all the sliced Skeletons in the right order"""
        coord_dict = {}

        for coord in self.coords_to_iterate:
            coord_value = self.dict_of_coords.get(coord)
            if coord_value is None:
                raise KeyError(
                    f"Cannot iterate over coord {coord}, since it does not exist: {self.dict_of_coords.keys()}"
                )
            else:
                coord_dict[coord] = coord_value

        coord_tuples = itertools.product(*[val for __, val in coord_dict.items()])
        list_of_skeletons = []
        for ctuple in coord_tuples:
            kwargs = {}
            for n, val in enumerate(ctuple):
                kwargs[list(coord_dict.keys())[n]] = val
            list_of_skeletons.append(self.skeleton.sel(**kwargs))

        return list_of_skeletons
