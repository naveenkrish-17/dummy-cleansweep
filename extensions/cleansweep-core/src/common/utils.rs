use log::debug;
use pyo3::{prelude::*, IntoPyObjectExt};
use pyo3::types::{PyDict, PyList};
use serde_json::Value as JSONValue;
use std::fs;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

/// Convert a serde_json::Value to a PyObject.
/// 
/// # Arguments
/// 
/// * `py` - A reference to the Python interpreter.
/// * `value` - A reference to the JSONValue to be converted.
/// 
/// # Returns
/// 
/// A PyObject representing the JSONValue.
/// 
/// # Examples
/// 
/// ```
/// let py = Python::acquire_gil();
/// let value = JSONValue::String("Hello".to_string());
/// let py_object = serde_value_to_pyobject(py, &value);
/// ```
pub fn serde_value_to_pyobject(py: Python, value: &JSONValue) -> PyObject {
  match value {
      JSONValue::Null => py.None(),
      JSONValue::Bool(b) => b.into_pyobject(py).unwrap().into_py_any(py).unwrap(),
      JSONValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_pyobject(py).unwrap().into_py_any(py).unwrap()
            } else if let Some(u) = n.as_u64() {
                u.into_pyobject(py).unwrap().into_py_any(py).unwrap()
            } else if let Some(f) = n.as_f64() {
                f.into_pyobject(py).unwrap().into_py_any(py).unwrap()
            } else {
                py.None() // This should rarely happen, as serde_json::Number can always be one of the three above
            }
      }
      JSONValue::String(s) => s.into_pyobject(py).unwrap().into_py_any(py).unwrap(),
      JSONValue::Array(arr) => {
            let py_list = PyList::empty(py);
            for item in arr {
                py_list.append(serde_value_to_pyobject(py, item)).unwrap();
            }
            py_list.into()
      }
      JSONValue::Object(obj) => {
            let py_dict = PyDict::new(py);
            for (key, value) in obj {
                py_dict.set_item(key, serde_value_to_pyobject(py, value)).unwrap();
            }
            py_dict.into()
      }
  }
}


/// Reads a JSON or NDJSON file and returns a JSONValue.
/// 
/// If the file ends with `.ndjson` or `.nd.json`, it is treated as newline-delimited JSON (NDJSON).
/// If the file ends with `.json`, the function first attempts to parse it as a standard JSON file.
/// If parsing fails, it then checks if it is NDJSON.
///
/// # Arguments
///
/// * `path` - A reference to the path of the JSON file.
///
/// # Returns
///
/// A JSONValue representing the JSON document.
///
/// # Examples
///
/// ```
/// let document = read_to_serde_value("data.json");
/// ```
pub fn read_to_serde_value(path: &str) -> JSONValue {
    let path = Path::new(path);

    let is_ndjson = path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext == "ndjson" || ext == "nd.json")
        .unwrap_or(false);

    // First, handle explicitly marked NDJSON files
    if is_ndjson {
        return read_ndjson(path);
    }

    // Read the file contents
    let content = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(e) => panic!("Error reading file {}: {}", path.display(), e),
    };

    // Attempt to parse as a single JSON object
    match serde_json::from_str::<JSONValue>(&content) {
        Ok(json) => json,
        Err(_) => {
            // If parsing as JSON fails, try as NDJSON
            debug!("File {} is not valid JSON, attempting NDJSON parsing.", path.display());
            read_ndjson(path)
        }
    }
}

/// Reads an NDJSON file and returns a JSONValue::Array
fn read_ndjson(path: &Path) -> JSONValue {
    let file = File::open(path).expect("Failed to open file");
    let reader = io::BufReader::new(file);

    let json_lines: Vec<JSONValue> = reader.lines()
        .filter_map(|line| {
            line.ok()
                .and_then(|l| serde_json::from_str::<JSONValue>(&l).ok())
        })
        .collect();

    JSONValue::Array(json_lines)
}
