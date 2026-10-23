from typing import Optional

def merge(skeletons_to_merge: list["Skeleton"], dim: Optional[str]=None):
    """Merges Skeletons of similar type together. Wrapper for .absorb()-metod
    
    dim: Optional[str] is the dimension over which to merge.
    
    If left unspecified, the dimension is decoded if unambiguous."""
    if len(skeletons_to_merge) == 1:
        return skeletons_to_merge[0]
    new_skeleton = skeletons_to_merge[0]
    return new_skeleton.absorb(skeletons_to_merge[1:], dim=dim)
