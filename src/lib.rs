use ::oxyroot::{Named, RootFile};
use numpy::IntoPyArray;
use pyo3::{exceptions::PyValueError, prelude::*, IntoPyObjectExt};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, StringArray, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use polars::functions::concat_df_diagonal;
use polars::prelude::*;
use pyo3_polars::PyDataFrame;
use rayon::prelude::*;

static POOL: Lazy<Mutex<rayon::ThreadPool>> = Lazy::new(|| {
    let num_threads = std::cmp::max(1, num_cpus::get() / 2);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .unwrap();
    Mutex::new(pool)
});

#[pyfunction]
fn set_num_threads(num_threads: usize) -> PyResult<()> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    *POOL.lock() = pool;
    Ok(())
}

#[pyclass(name = "RootFile")]
struct PyRootFile {
    #[pyo3(get)]
    path: String,
}

#[pyclass(name = "Tree")]
struct PyTree {
    #[pyo3(get)]
    path: String,
    #[pyo3(get)]
    name: String,
}

#[pyclass(name = "Branch")]
struct PyBranch {
    #[pyo3(get)]
    path: String,
    #[pyo3(get)]
    tree_name: String,
    #[pyo3(get)]
    name: String,
}

fn tree_to_dataframe(
    tree: &::oxyroot::ReaderTree,
    columns: Option<Vec<String>>,
    ignore_columns: Option<Vec<String>>,
) -> PyResult<DataFrame> {
    let mut branches_to_save = if let Some(columns) = columns {
        columns
    } else {
        tree.branches().map(|b| b.name().to_string()).collect()
    };

    if let Some(ignore_columns) = ignore_columns {
        branches_to_save.retain(|c| !ignore_columns.contains(c));
    }

    let mut series_vec = Vec::new();

    for branch_name in branches_to_save {
        let branch = match tree.branch(&branch_name) {
            Some(branch) => branch,
            None => {
                println!("Branch '{}' not found, skipping", branch_name);
                continue;
            }
        };

        let series = match branch.item_type_name().as_str() {
            "float" => {
                let data = branch.as_iter::<f32>().unwrap().collect::<Vec<_>>();
                Series::new((&branch_name).into(), data)
            }
            "double" => {
                let data = branch.as_iter::<f64>().unwrap().collect::<Vec<_>>();
                Series::new((&branch_name).into(), data)
            }
            "int32_t" => {
                let data = branch.as_iter::<i32>().unwrap().collect::<Vec<_>>();
                Series::new((&branch_name).into(), data)
            }
            "int64_t" => {
                let data = branch.as_iter::<i64>().unwrap().collect::<Vec<_>>();
                Series::new((&branch_name).into(), data)
            }
            "uint32_t" => {
                let data = branch.as_iter::<u32>().unwrap().collect::<Vec<_>>();
                Series::new((&branch_name).into(), data)
            }
            "uint64_t" => {
                let data = branch.as_iter::<u64>().unwrap().collect::<Vec<_>>();
                Series::new((&branch_name).into(), data)
            }
            "string" => {
                let data = branch.as_iter::<String>().unwrap().collect::<Vec<_>>();
                Series::new((&branch_name).into(), data)
            }
            other => {
                println!("Unsupported branch type: {}, skipping", other);
                continue;
            }
        };
        series_vec.push(series);
    }

    DataFrame::new(series_vec.into_iter().map(|s| s.into()).collect())
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pymethods]
impl PyRootFile {
    #[new]
    fn new(path: String) -> Self {
        PyRootFile { path }
    }

    fn keys(&self) -> PyResult<Vec<String>> {
        let file = RootFile::open(&self.path).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(file
            .keys()
            .into_iter()
            .map(|k| k.name().to_string())
            .collect())
    }

    fn __getitem__(&self, name: &str) -> PyResult<PyTree> {
        Ok(PyTree {
            path: self.path.clone(),
            name: name.to_string(),
        })
    }
}

#[pymethods]
impl PyTree {
    fn branches(&self) -> PyResult<Vec<String>> {
        let mut file =
            RootFile::open(&self.path).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let tree = file
            .get_tree(&self.name)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(tree.branches().map(|b| b.name().to_string()).collect())
    }

    fn __getitem__(&self, name: &str) -> PyResult<PyBranch> {
        Ok(PyBranch {
            path: self.path.clone(),
            tree_name: self.name.clone(),
            name: name.to_string(),
        })
    }

    fn __iter__(slf: PyRef<Self>) -> PyResult<Py<PyBranchIterator>> {
        let branches = slf.branches()?;
        Py::new(
            slf.py(),
            PyBranchIterator {
                path: slf.path.clone(),
                tree_name: slf.name.clone(),
                branches: branches.into_iter(),
            },
        )
    }

    #[pyo3(signature = (columns = None, ignore_columns = None))]
    fn arrays(
        &self,
        columns: Option<Vec<String>>,
        ignore_columns: Option<Vec<String>>,
    ) -> PyResult<PyDataFrame> {
        let mut file =
            RootFile::open(&self.path).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let tree = file
            .get_tree(&self.name)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let df = tree_to_dataframe(&tree, columns, ignore_columns)?;
        Ok(PyDataFrame(df))
    }

    #[pyo3(signature = (output_file, overwrite = false, compression = "snappy", columns = None))]
    fn to_parquet(
        &self,
        output_file: String,
        overwrite: bool,
        compression: &str,
        columns: Option<Vec<String>>,
    ) -> PyResult<()> {
        if !overwrite && Path::new(&output_file).exists() {
            return Err(PyValueError::new_err("File exists, use overwrite=True"));
        }

        let compression = match compression {
            "snappy" => Compression::SNAPPY,
            "uncompressed" => Compression::UNCOMPRESSED,
            "gzip" => Compression::GZIP(GzipLevel::default()),
            "lzo" => Compression::LZO,
            "brotli" => Compression::BROTLI(BrotliLevel::default()),
            "lz4" => Compression::LZ4,
            "zstd" => Compression::ZSTD(ZstdLevel::default()),
            _ => return Err(PyValueError::new_err("Invalid compression type")),
        };

        let mut file =
            RootFile::open(&self.path).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let tree = file
            .get_tree(&self.name)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let mut fields = Vec::new();
        let mut arrays = Vec::new();

        let branches_to_save = if let Some(columns) = columns {
            columns
        } else {
            tree.branches().map(|b| b.name().to_string()).collect()
        };

        for branch_name in branches_to_save {
            let branch = match tree.branch(&branch_name) {
                Some(branch) => branch,
                None => {
                    println!("Branch '{}' not found, skipping", branch_name);
                    continue;
                }
            };

            let (field, array) = match branch.item_type_name().as_str() {
                "float" => {
                    let data = branch.as_iter::<f32>().unwrap().collect::<Vec<_>>();
                    let array: ArrayRef = Arc::new(Float32Array::from(data));
                    (Field::new(&branch_name, DataType::Float32, false), array)
                }
                "double" => {
                    let data = branch.as_iter::<f64>().unwrap().collect::<Vec<_>>();
                    let array: ArrayRef = Arc::new(Float64Array::from(data));
                    (Field::new(&branch_name, DataType::Float64, false), array)
                }
                "int32_t" => {
                    let data = branch.as_iter::<i32>().unwrap().collect::<Vec<_>>();
                    let array: ArrayRef = Arc::new(Int32Array::from(data));
                    (Field::new(&branch_name, DataType::Int32, false), array)
                }
                "int64_t" => {
                    let data = branch.as_iter::<i64>().unwrap().collect::<Vec<_>>();
                    let array: ArrayRef = Arc::new(Int64Array::from(data));
                    (Field::new(&branch_name, DataType::Int64, false), array)
                }
                "uint32_t" => {
                    let data = branch.as_iter::<u32>().unwrap().collect::<Vec<_>>();
                    let array: ArrayRef = Arc::new(UInt32Array::from(data));
                    (Field::new(&branch_name, DataType::UInt32, false), array)
                }
                "uint64_t" => {
                    let data = branch.as_iter::<u64>().unwrap().collect::<Vec<_>>();
                    let array: ArrayRef = Arc::new(UInt64Array::from(data));
                    (Field::new(&branch_name, DataType::UInt64, false), array)
                }
                "string" => {
                    let data = branch.as_iter::<String>().unwrap().collect::<Vec<_>>();
                    let array: ArrayRef = Arc::new(StringArray::from(data));
                    (Field::new(&branch_name, DataType::Utf8, false), array)
                }
                other => {
                    println!("Unsupported branch type: {}, skipping", other);
                    continue;
                }
            };
            fields.push(field);
            arrays.push(array);
        }

        let schema = Arc::new(Schema::new(fields));
        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

        let file = File::create(output_file)?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        writer
            .write(&batch)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        writer
            .close()
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        Ok(())
    }
}

#[pyclass]
struct PyBranchIterator {
    path: String,
    tree_name: String,
    branches: std::vec::IntoIter<String>,
}

#[pymethods]
impl PyBranchIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(&mut self) -> Option<PyBranch> {
        self.branches.next().map(|name| PyBranch {
            path: self.path.clone(),
            tree_name: self.tree_name.clone(),
            name,
        })
    }
}

#[pymethods]
impl PyBranch {
    fn array(&self, py: Python) -> PyResult<Py<PyAny>> {
        let mut file =
            RootFile::open(&self.path).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let tree = file
            .get_tree(&self.tree_name)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let branch = tree
            .branch(&self.name)
            .ok_or_else(|| PyValueError::new_err("Branch not found"))?;

        match branch.item_type_name().as_str() {
            "float" => {
                let data = branch
                    .as_iter::<f32>()
                    .map_err(|e| PyValueError::new_err(e.to_string()))?
                    .collect::<Vec<_>>();
                Ok(data.into_pyarray(py).into())
            }
            "double" => {
                let data = branch
                    .as_iter::<f64>()
                    .map_err(|e| PyValueError::new_err(e.to_string()))?
                    .collect::<Vec<_>>();
                Ok(data.into_pyarray(py).into())
            }
            "int32_t" => {
                let data = branch
                    .as_iter::<i32>()
                    .map_err(|e| PyValueError::new_err(e.to_string()))?
                    .collect::<Vec<_>>();
                Ok(data.into_pyarray(py).into())
            }
            "int64_t" => {
                let data = branch
                    .as_iter::<i64>()
                    .map_err(|e| PyValueError::new_err(e.to_string()))?
                    .collect::<Vec<_>>();
                Ok(data.into_pyarray(py).into())
            }
            "uint32_t" => {
                let data = branch
                    .as_iter::<u32>()
                    .map_err(|e| PyValueError::new_err(e.to_string()))?
                    .collect::<Vec<_>>();
                Ok(data.into_pyarray(py).into())
            }
            "uint64_t" => {
                let data = branch
                    .as_iter::<u64>()
                    .map_err(|e| PyValueError::new_err(e.to_string()))?
                    .collect::<Vec<_>>();
                Ok(data.into_pyarray(py).into())
            }
            "string" => {
                let data = branch
                    .as_iter::<String>()
                    .map_err(|e| PyValueError::new_err(e.to_string()))?
                    .collect::<Vec<_>>();
                Ok(data.into_py_any(py).unwrap())
            }
            other => Err(PyValueError::new_err(format!(
                "Unsupported branch type: {}",
                other
            ))),
        }
    }

    #[getter]
    fn typename(&self) -> PyResult<String> {
        let mut file =
            RootFile::open(&self.path).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let tree = file
            .get_tree(&self.tree_name)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let branch = tree
            .branch(&self.name)
            .ok_or_else(|| PyValueError::new_err("Branch not found"))?;
        Ok(branch.item_type_name())
    }
}

#[pyfunction]
fn open(path: String) -> PyResult<PyRootFile> {
    Ok(PyRootFile::new(path))
}

#[pyfunction]
fn version() -> PyResult<String> {
    Ok(env!("CARGO_PKG_VERSION").to_string())
}

#[pyfunction]
#[pyo3(signature = (paths, tree_name, columns = None, ignore_columns = None))]
fn concat_trees(
    paths: Vec<String>,
    tree_name: String,
    columns: Option<Vec<String>>,
    ignore_columns: Option<Vec<String>>,
) -> PyResult<PyDataFrame> {
    let mut all_paths = Vec::new();
    for path in paths {
        for entry in glob::glob(&path).map_err(|e| PyValueError::new_err(e.to_string()))? {
            match entry {
                Ok(path) => {
                    all_paths.push(path.to_str().unwrap().to_string());
                }
                Err(e) => return Err(PyValueError::new_err(e.to_string())),
            }
        }
    }

    let pool = POOL.lock();
    let dfs: Vec<DataFrame> = pool.install(|| {
        all_paths
            .par_iter()
            .map(|path| {
                let mut file =
                    RootFile::open(path).map_err(|e| PyValueError::new_err(e.to_string()))?;
                let tree = file
                    .get_tree(&tree_name)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?;
                tree_to_dataframe(&tree, columns.clone(), ignore_columns.clone())
            })
            .filter_map(Result::ok)
            .collect()
    });

    if dfs.is_empty() {
        return Ok(PyDataFrame(DataFrame::default()));
    }

    let combined_df = concat_df_diagonal(&dfs).map_err(|e| PyValueError::new_err(e.to_string()))?;

    Ok(PyDataFrame(combined_df))
}

/// A Python module to read root files, implemented in Rust.
#[pymodule]
fn oxyroot(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add_function(wrap_pyfunction!(concat_trees, m)?)?;
    m.add_function(wrap_pyfunction!(set_num_threads, m)?)?;
    m.add_class::<PyRootFile>()?;
    m.add_class::<PyTree>()?;
    m.add_class::<PyBranch>()?;
    m.add_class::<PyBranchIterator>()?;
    Ok(())
}
