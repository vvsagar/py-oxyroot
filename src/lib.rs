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
use parquet::arrow::ArrowWriter;

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

    #[pyo3(signature = (output_file, overwrite = false))]
    fn to_parquet(&self, output_file: String, overwrite: bool) -> PyResult<()> {
        if !overwrite && Path::new(&output_file).exists() {
            return Err(PyValueError::new_err("File exists, use overwrite=True"));
        }

        let mut file =
            RootFile::open(&self.path).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let tree = file
            .get_tree(&self.name)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let mut fields = Vec::new();
        let mut columns = Vec::new();

        for branch in tree.branches() {
            let branch_name = branch.name().to_string();
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
            columns.push(array);
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

        let file = File::create(output_file)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
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

/// A Python module to read root files, implemented in Rust.
#[pymodule]
fn oxyroot(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add_class::<PyRootFile>()?;
    m.add_class::<PyTree>()?;
    m.add_class::<PyBranch>()?;
    m.add_class::<PyBranchIterator>()?;
    Ok(())
}
