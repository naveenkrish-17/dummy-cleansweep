
use crate::common::utils::serde_value_to_pyobject;
use pyo3::ffi::PyObject;
use pyo3::{prelude::*, IntoPyObjectExt};
use pyo3::types::PyDict;
use serde::{Serialize, Deserialize};
use serde_json::Value as JSONValue;
use std::{convert::Infallible, option::Option};
use uuid::Uuid;


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableModel {
    pub title: Option<String>,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub metadata: Option<JSONValue>,
}


impl<'a, 'py> IntoPyObject<'py> for &'a TableModel {
    type Target = PyDict;
    type Output = Bound<'py, Self::Target>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);
        dict.set_item("title", &self.title).unwrap();
        dict.set_item("columns", &self.columns).unwrap();
        dict.set_item("rows", &self.rows).unwrap();
        if self.metadata.is_some() {
            dict.set_item("metadata", serde_value_to_pyobject(py, &self.metadata.clone().unwrap())).unwrap();
        }
        Ok(dict)
    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContentModel {
    pub title: Option<String>,
    pub data: Vec<ContentData>, // Custom enum for data type
    pub metadata: Option<JSONValue>,
}

impl<'a, 'py> IntoPyObject<'py> for &'a ContentModel {
    type Target = PyDict;
    type Output = Bound<'py, Self::Target>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);
        dict.set_item("title", &self.title).unwrap();
        dict.set_item("data", &self.data).unwrap();
        if self.metadata.is_some() {
            dict.set_item("metadata", serde_value_to_pyobject(py, &self.metadata.clone().unwrap())).unwrap();
        }
        Ok(dict)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum ContentData {
    Text(String),
    ContentModel(Box<ContentModel>), // Recursive type requires Box
    TableModel(TableModel),
}

impl<'a, 'py> IntoPyObject<'py> for &'a ContentData {
    type Target = PyAny;
    type Output = Bound<'py, Self::Target>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {

        fn convert_to_pyobject<'py, T>(
            py: Python<'py>,
            value: Bound<'py, T>
        ) -> Result<Bound<'py, PyAny>, Infallible>
        where
            Bound<'py, T>: IntoPyObjectExt<'py>, {
            let inner: *mut PyObject = value.as_ptr();
            let bound = unsafe { Bound::from_owned_ptr(py, inner) };
            Ok(bound)
        }

        match self {
            ContentData::Text(text) => convert_to_pyobject(py, text.into_pyobject(py).unwrap()),
            ContentData::ContentModel(content) => convert_to_pyobject(py, content.into_pyobject(py).unwrap()),
            ContentData::TableModel(table) => convert_to_pyobject(py, table.into_pyobject(py).unwrap()),
        }
    }
}



#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DocumentModel {
    pub name: String,
    pub id: String,
    pub metadata: JSONValue,
    pub content: Vec<ContentData>,
}

impl Default for DocumentModel {
    fn default() -> Self {
        DocumentModel {
            name: String::new(),
            id: Uuid::new_v4().to_string(),
            metadata: JSONValue::default(),
            content: vec![],
        }
    }
}

impl<'a, 'py> IntoPyObject<'py> for &'a DocumentModel {
    type Target = PyDict;
    type Output = Bound<'py, Self::Target>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);
        dict.set_item("id", &self.id).unwrap();
        dict.set_item("metadata", serde_value_to_pyobject(py, &self.metadata)).unwrap();
        dict.set_item("content", &self.content).unwrap();
        Ok(dict)
    }
}
