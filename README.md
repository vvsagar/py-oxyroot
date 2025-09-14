# Python bindings for oxyroot 

[![CI](https://github.com/vvsagar/py-oxyroot/actions/workflows/CI.yml/badge.svg)](https://github.com/vvsagar/py-oxyroot/actions/workflows/CI.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)

> **Warning**
> This project is an early prototype and is not yet recommended for production use. For a mature and well-tested alternative, please consider using [uproot](https://github.com/scikit-hep/uproot5).

A fast, Rust-powered Python reader for CERN ROOT files.

This python package provides simple bindings to [`oxyroot`, a rust package](https://github.com/m-dupont/oxyroot), to read data from `.root` files, inspired by libraries like `uproot`. It leverages the speed of Rust and integrates with the scientific Python ecosystem by providing data as NumPy arrays or polars dataframes.

## Features

- Simple API similar to `uproot`
- Core logic is written in Rust.
- Get branch data directly as NumPy arrays or Polars dataframe.
- Parquet Conversion: Convert TTrees directly to Apache Parquet files with a single command.

## Quick Start

Here's how to open a ROOT file, access a TTree, and read a TBranch into a NumPy array.

```python
import oxyroot
import numpy as np

# Open the ROOT file
file = oxyroot.open("ntuples.root")

# Get a TTree
tree = file["mu_mc"]

# List branches in the tree
print(f"Branches: {tree.branches()}")

# Get a specific branch and its data as a NumPy array
branch = tree["mu_pt"]
data = branch.array()

print(f"Read branch '{branch.name}' into a {type(data)}")
print(f"Mean value: {np.nanmean(data):.2f}")
```

## Converting to Parquet

You can easily convert all (or a subset of) branches in a TTree to a Parquet file.

```python
# Convert the entire tree to a Parquet file
tree.to_parquet("output.parquet")

# Convert specific columns to a Parquet file with ZSTD compression
tree.to_parquet(
    "output_subset.parquet",
    columns=["mu_pt", "mu_eta"],
    compression="zstd"
)
```

## Performance

`oxyroot` is intended to be fast. Here is a simple benchmark comparing the time taken to read all branches of a TTree with `uproot` and `oxyroot`.

```python
import oxyroot
import uproot
import time

file_name = "ntuples.root"
tree_name = 'mu_mc'

# Time uproot
start_time = time.time()
up_tree = uproot.open(file_name)[tree_name]
for branch in up_tree:
    if branch.typename != "std::string":
        branch.array(library="np")
end_time = time.time()
print(f"Uproot took: {end_time - start_time:.3f}s")

# Time oxyroot
start_time = time.time()
oxy_tree = oxyroot.open(file_name)[tree_name]
for branch in oxy_tree:
    branch.array()
end_time = time.time()
print(f"Oxyroot took: {end_time - start_time:.3f}s")
```

On a small file (~20 MB) with multiple data formats, oxyroot took half the time of uproot, and also read in the branch with strings!

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue.
