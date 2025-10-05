from typing import Iterator, List, Optional
import polars as pl

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
    def arrays(self, columns:Optional[List[str]] = None, ignore_columns: Optional[List[str]] = None) -> pl.DataFrame ...
    def to_parquet(self, output_file: str, overwrite: bool = False, compression: str = "snappy", columns: Optional[List[str]] = None) -> None: ...

class Branch:
    path: str
    tree_name: str
    name: str
    def array(self) -> pl.Series: ...
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

def concat_trees(
    paths: List[str],
    tree_name: str,
    columns: Optional[List[str]] = None,
    ignore_columns: Optional[List[str]] = None,
) -> pl.DataFrame:
    """
    Reads multiple ROOT files, concatenates the specified tree, and returns a single Polars DataFrame.

    Args:
        paths: A list of paths to the ROOT files. Wildcards are supported.
        tree_name: The name of the tree to read from each file.
        columns: An optional list of column names to include. If None, all columns are included.
        ignore_columns: An optional list of column names to exclude.

    Returns:
        A single Polars DataFrame containing the concatenated data.
    """
    ...

def set_num_threads(num_threads: int) -> None:
    """
    Sets the number of threads to use for parallel operations.

    Args:
        num_threads: The number of threads to use.
    """
    ...
