use crate::common::utils::{read_to_serde_value, serde_value_to_pyobject};
use log::debug;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde_json::Value as JSONValue;

pub type PyToken = (String, PyObject);
pub type Token = (String, JSONValue);

/// Tokenizer struct that tokenizes a JSON document.
pub struct Tokenizer;

impl Tokenizer {

    fn tokenize_list(list: Vec<JSONValue>, root: Option<String>) -> Vec<Token> {

        let root = root.unwrap_or_else(|| "$".to_string());
        // let list = document.as_array().unwrap();
        // if all items in the document (array) are objects, then tokenize the items returning
        // a vector of tokens
        if list.iter().all(|item| item.is_object()) {
            // tokenize the items returning a vector of tokens
            let mut tokens: Vec<Token> = Vec::new();
            for (index, value) in list.iter().enumerate() {
                let path = format!("{}.{}", root, index);
                let nested_tokens = Tokenizer::tokenize(value, Some(path.clone()));
                tokens.extend(nested_tokens);
            }
            return tokens;
        }

        // if all items are lists then recursively tokenize the items
        if list.iter().all(|item| item.is_object()) {
            // tokenize the items returning a vector of tokens
            let mut tokens: Vec<Token> = Vec::new();
            for (index, value) in list.iter().enumerate() {
                let path = format!("{}.{}", root, index);
                let nested_tokens = Tokenizer::tokenize_list(value.as_array().unwrap().clone(), Some(path.clone()));
                tokens.extend(nested_tokens);
            }
            return tokens;
        }

        // return the array with it's root
        vec![(root, JSONValue::Array(list))]

    }

    /// Tokenizes a JSON document into a vector of tokens.
    ///
    /// # Arguments
    ///
    /// * `document` - A reference to the JSONValue document to be tokenized.
    /// * `root` - An optional string representing the root path of the document. If not provided, the root path is set to "$".
    ///
    /// # Returns
    ///
    /// A vector of tokens representing the tokenized JSON document.
    ///
    /// # Examples
    ///
    /// ```
    /// use json::JsonValue;
    ///
    /// let document = JSONValue::from_string(r#"{"name": "John", "age": 30}"#).unwrap();
    /// let tokens = Tokenizer::tokenize(&document, None);
    /// assert_eq!(tokens.len(), 2);
    /// ```
    fn tokenize(document: &JSONValue, root: Option<String>) -> Vec<Token> {
        let root = root.unwrap_or_else(|| "$".to_string());
        let mut tokens: Vec<Token> = Vec::new();

        match document {
            JSONValue::Object(map) => {
            for (key, value) in map.iter() {
                let key = key.to_string();
                let path = format!("{}.{}", root, key);
                let nested_tokens = Tokenizer::tokenize(value, Some(path.clone()));
                tokens.extend(nested_tokens);
            }
            }
            JSONValue::Array(list) => {
                // for (index, value) in list.iter().enumerate() {
                //     let path = format!("{}.{}", root, index);
                //     let nested_tokens = Tokenizer::tokenize(value, Some(path.clone()));
                //     tokens.extend(nested_tokens);
                // }
                tokens.extend(Tokenizer::tokenize_list(list.clone(), Some(root)));
            }
            _ => {
                tokens.push((root, document.clone()));
            }
        }
        tokens.sort_by_key(|(key, _)| key.clone());
        tokens
    }


    /// Tokenize a JSON document and return a vector of tokens.
    /// 
    /// # Arguments
    /// 
    /// * `path` - A reference to the path of the JSON document.
    /// * `root` - An optional string representing the root path of the document.
    /// 
    /// # Returns
    /// 
    /// A vector of tokens representing the tokenized JSON document.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let tokenizer = Tokenizer::new();
    /// let tokens = tokenizer.tokenize_document("data.json", None);
    /// ```
    pub fn tokenize_document(path: &str, root: &Option<String>) -> PyResult<Vec<Vec<Token>>> {
        let mut document: JSONValue = read_to_serde_value(path);

        if root.is_some() {
            let path = root.clone().unwrap().replace(".", "/").replace("[", "/").replace("]", "").replace("$", "");
            debug!("Pointer: {}", path);
            document = document.pointer(&path).unwrap().clone();
        }

        let mut tokens: Vec<Vec<Token>> = Vec::new();

        match document {
            JSONValue::Object(_) => {
            tokens.push(Tokenizer::tokenize(&document, Some("$".to_string())));
            }
            JSONValue::Array(arr) => {
            for value in arr.iter() {
                tokens.push(Tokenizer::tokenize(value, Some("$".to_string())));
            }
            }
            _ => {
            return Err(PyValueError::new_err("Invalid JSON document"));
            }
        }

        Ok(tokens)
      
    }

}


/// Python implementation of the Tokenizer class
#[pyclass(module="cleansweep_core._cleansweep_core", name="Tokenizer")]
pub struct PyTokenizer;

#[pymethods]
impl PyTokenizer {

    /// Create a new instance of the Tokenizer class.
    #[new]
    fn new() -> Self {
        PyTokenizer
    }
    
    /// Tokenize a JSON document and return a vector of tokens.
    /// 
    /// # Arguments
    /// 
    /// * `path` - A reference to the path of the JSON document.
    /// * `root` - An optional string representing the root path of the document.
    /// 
    /// # Returns
    /// 
    /// A vector of tokens representing the tokenized JSON document.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let tokenizer = Tokenizer::new();
    /// let tokens = tokenizer.tokenize_document("data.json", None);
    /// ```
    #[pyo3(signature = (path, root=None))]
    pub fn tokenize_document(&self, py: Python, path: String, root: Option<String>) -> PyResult<Vec<Vec<PyToken>>> {
        let tokens = Tokenizer::tokenize_document(&path, &root).unwrap();

        Ok(tokens.iter().map(|t| {
            t.iter().map(|(key, value)| {
                (key.clone(), serde_value_to_pyobject(py, value))
            }).collect()
        }).collect())
      
    }

}
