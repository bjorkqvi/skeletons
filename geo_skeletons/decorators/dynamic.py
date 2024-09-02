from copy import deepcopy


def dynamic(c):
    c.core = deepcopy(c.core)
    c.core.static = False
    return c
