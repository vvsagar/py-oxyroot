use ::oxyroot::{Named, RootFile};
use numpy::IntoPyArray;
use pyo3::{exceptions::PyValueError, prelude::*, IntoPyObjectExt};

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
