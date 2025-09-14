from typing import Iterator, List, Optional
import numpy as np

class RootFile:
    path: str
    def __init__(self, path: str) -> None: ...
    def keys(self) -> List[str]: ...
    def __getitem__(self, name: str) -> Tree: ...

class Tree:
    path: str
    name: str
    def branches(self) -> List[str]: ...
    def __getitem__(self, name: str) -> Branch: ...
    def __iter__(self) -> Iterator[Branch]: ...
    def to_parquet(self, output_file: str, overwrite: bool = False, compression: str = "snappy", columns: Optional[List[str]] = None) -> None: ...

class Branch:
    path: str
    tree_name: str
    name: str
    def array(self) -> np.ndarray: ...
    @property
    def typename(self) -> str: ...

class BranchIterator:
    def __iter__(self) -> "BranchIterator": ...
    def __next__(self) -> Optional[Branch]: ...

def open(
    path: str,
) -> RootFile:
    """
    Opens a ROOT file.

    Args:
        path: Path of the ROOT file.

    Returns:
        A RootFile object.
    """
    ...
