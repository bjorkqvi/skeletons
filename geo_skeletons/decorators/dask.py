from typing import Union


def activate_dask(chunks: Union[tuple[int], str] = "auto"):
    def wrapper(c):
        c.chunks = chunks
        return c

    return wrapper
