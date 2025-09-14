use ::oxyroot::{Named, RootFile};
use numpy::ToPyArray;
use pyo3::{exceptions::PyValueError, prelude::*, types::PyModule, IntoPyObjectExt};

#[pyfunction]
fn version() -> PyResult<String> {
    Ok(env!("CARGO_PKG_VERSION").to_string())
}

/// Read a ROOT file and return the list of trees, the branches of a tree, or the values of a branch.
#[pyfunction]
#[pyo3(signature = (path, tree_name = None, branch = None))]
fn read_root(
    path: String,
    tree_name: Option<String>,
    branch: Option<String>,
) -> PyResult<Py<PyAny>> {
    let mut file = RootFile::open(&path).unwrap();
    let keys: Vec<String> = file
        .keys()
        .into_iter()
        .map(|k| k.name().to_string())
        .collect();

    Python::attach(|py| -> PyResult<Py<PyAny>> {
        match tree_name {
            Some(name) => {
                if let Ok(tree) = file.get_tree(&name) {
                    let branches_available: Vec<String> =
                        tree.branches().map(|b| b.name().to_string()).collect();

                    match branch {
                        Some(bs) => {
                            if let Some(branch) = tree.branch(&bs) {
                                match branch.item_type_name().as_str() {
                                    "f32" => {
                                        let data =
                                            branch.as_iter::<f32>().unwrap().collect::<Vec<_>>();
                                        Ok(data.to_pyarray(py).into_py_any(py).unwrap())
                                    }
                                    "double" => {
                                        let data =
                                            branch.as_iter::<f64>().unwrap().collect::<Vec<_>>();
                                        Ok(data.to_pyarray(py).into_py_any(py).unwrap())
                                    }
                                    "int32_t" => {
                                        let data =
                                            branch.as_iter::<i32>().unwrap().collect::<Vec<_>>();
                                        Ok(data.to_pyarray(py).into_py_any(py).unwrap())
                                    }
                                    "int64_t" => {
                                        let data =
                                            branch.as_iter::<i64>().unwrap().collect::<Vec<_>>();
                                        Ok(data.to_pyarray(py).into_py_any(py).unwrap())
                                    }
                                    "uint32_t" => {
                                        let data =
                                            branch.as_iter::<u32>().unwrap().collect::<Vec<_>>();
                                        Ok(data.to_pyarray(py).into_py_any(py).unwrap())
                                    }
                                    "uint64_t" => {
                                        let data =
                                            branch.as_iter::<u64>().unwrap().collect::<Vec<_>>();
                                        Ok(data.to_pyarray(py).into_py_any(py).unwrap())
                                    }
                                    "string" => {
                                        let data =
                                            branch.as_iter::<String>().unwrap().collect::<Vec<_>>();
                                        Ok(data.into_py_any(py).unwrap())
                                    }
                                    other => Err(PyValueError::new_err(format!(
                                        "Unsupported branch type: {}",
                                        other
                                    ))),
                                }
                            } else {
                                Err(PyValueError::new_err(format!(
                                    "Branch '{}' not found. Available branches are: {:?}",
                                    bs, branches_available
                                )))
                            }
                        }
                        None => Ok(branches_available.into_py_any(py).unwrap()),
                    }
                } else {
                    Err(PyValueError::new_err(format!(
                        "Tree '{}' not found. Available trees are: {:?}",
                        name, keys
                    )))
                }
            }
            None => Ok(keys.into_py_any(py).unwrap()),
        }
    })
}

/// A Python module to read root files implemented in Rust.
#[pymodule]
fn oxyroot(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    m.add_function(wrap_pyfunction!(read_root, m)?)?;
    Ok(())
}
