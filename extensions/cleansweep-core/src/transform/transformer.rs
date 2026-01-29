use log::debug;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rayon::prelude::*;
use regex::Regex;
use crate::tokenize::tokenizer::{Token, Tokenizer};
use serde_json::{Map, Value as JSONValue};
use crate::common::utils::read_to_serde_value;
use crate::common::utils::serde_value_to_pyobject;



/// Rust implementation of the Transformer class
pub struct Transformer;

impl Transformer {

    /// Search a vector of tokens for a specific mapping.
    /// 
    /// # Arguments
    /// 
    /// * `tokens` - A reference to a vector of tokens.
    /// * `mapping` - A reference to a string representing the mapping to search for.
    /// * `first` - A boolean value indicating whether to return the first match.
    /// 
    /// # Returns
    /// 
    /// An optional vector of JSONValues representing the search results.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let tokens = vec![("name".to_string(), JSONValue::String("John".to_string()))];
    /// let mapping = "name".to_string();
    /// let results = Transformer::token_search(&tokens, &mapping, false);
    /// assert_eq!(results.unwrap().len(), 1);
    /// ```
    fn token_search(tokens: &[Token], mapping: &str, first: bool) -> Option<Vec<JSONValue>> {

        let mut search_key = mapping.to_owned();
        let mut r = false;
        let mut re: Option<Regex> = None;
        if search_key.contains("[*]") {
            r = true;
            search_key = search_key
            .replace("[*]", ".\\[\\d+\\]")
            .replace("$", "\\$")
            .replace(".", "\\.")
            ;
            re = Some(Regex::new(&search_key).unwrap());
        }
        // debug!("search key: {}", search_key);
        let mut results: Vec<JSONValue> = Vec::new();
        for (path, value) in tokens.iter() {

            if r{
                if re.as_mut().unwrap().is_match(path) {
                    results.push(value.clone());
                    if first {
                        break;
                    }
                }
            } else if path == mapping {
                results.push(value.clone());
                if first {
                    break;
                }
            }
        }
        if results.is_empty() {
            return None;
        }
        Some(results)
    }

    /// Reduce the number of tokens to search for a mapping.
    /// 
    /// # Arguments
    /// 
    /// * `tokens` - A reference to a vector of tokens.
    /// * `mapping` - A reference to a string representing the mapping to search for.
    /// 
    /// # Returns
    /// 
    /// A vector of tokens representing the reduced search space.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let tokens = vec![("name".to_string(), JSONValue::String("John".to_string()))];
    /// let mapping = "name".to_string();
    /// let reduced_tokens = Transformer::reduce_tokens(&tokens, &mapping);
    /// assert_eq!(reduced_tokens.len(), 1);
    /// ```
    fn reduce_tokens(tokens: &[Token], mapping: &str) -> Vec<Token> {
        let mut mid = tokens.len() / 2;
        let mut low = 0;
        let mut q_low = 0;
        let mut high = tokens.len();
        let mut q_high = tokens.len();

        let map = mapping.to_owned();

        loop {
            if tokens[mid].0 < map {
                q_low = mid;
            }

            if tokens[mid].0 > map {
                q_high = mid;
            }

            if !(tokens[mid].0 < map && tokens[mid].0 > map) {
                return tokens[low..high].to_vec();
            }

            low = q_low;
            high = q_high;
            mid = (low + high) / 2;
        }
        
    }

    /// Returns all tokens that match a mapping.
    /// 
    /// # Arguments
    /// 
    /// * `tokens` - A reference to a vector of tokens.
    /// * `mapping` - A reference to a JSONValue representing the mapping.
    /// 
    /// # Returns
    /// 
    /// A vector of strings representing the keys of the tokens that match the mapping.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let tokens = vec![("name".to_string(), JSONValue::String("John".to_string()))];
    /// let mapping = JSONValue::String("name".to_string());
    /// let keys = Transformer::get_all_token_keys(&tokens, &mapping);
    /// assert_eq!(keys.len(), 1);
    /// ```
    fn get_all_tokens(tokens: &[Token], mapping: &JSONValue) -> Vec<Token> {
        let mut keys: Vec<Token> = Vec::new();
        for (_, value) in mapping.as_object().unwrap() {
            if value.is_object() {
                // debug!("Mapping is an object.");
                keys.extend(Transformer::get_all_tokens(tokens, value));
            } else {
                let mut search_key = value.as_str().unwrap().to_owned();
                let mut r = false;
                let mut re: Option<Regex> = None;
                let reduced_tokens = Transformer::reduce_tokens(tokens, &search_key);
                if search_key.contains("[*]") {
                    r = true;
                    search_key = search_key
                    .replace("[*]", ".\\d+")
                    .replace("$", "\\$")
                    .replace(".", "\\.")
                    ;
                    re = Some(Regex::new(&search_key).unwrap());
                }
                for token in reduced_tokens.iter() {
                    if r {
                        if re.as_mut().unwrap().is_match(&token.0) {
                            keys.push(token.clone());
                        }
                    } else if token.0 == search_key {
                            keys.push(token.clone());
                    }
                }
            }
        }
        keys
    }

    /// Recursive function to search for values when the mapping contains an array.
    /// 
    /// # Arguments
    /// 
    /// * `tokens` - A reference to a vector of tokens.
    /// * `mapping` - A reference to a string representing the mapping to search for.
    /// * `pattern` - A reference to a string representing the maximum array indexes to search for.
    /// 
    /// # Returns
    /// 
    /// A vector of optional JSONValues representing the search results.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let tokens = vec![("name".to_string(), JSONValue::String("John".to_string()))];
    /// let mapping = "name".to_string();
    /// let pattern = "0";
    /// let results = Transformer::array_search(&tokens, &mapping, &pattern);
    /// assert_eq!(results.len(), 1);
    /// ```
    fn array_search(tokens: &[Token], mapping: &str, pattern: &str) -> Vec<Option<JSONValue>> {
        // debug!("Array search - mapping: {}, pattern: {}", mapping, pattern);
        let mut results: Vec<Option<JSONValue>> = Vec::new();
        
        let indexes = pattern.split("|").collect::<Vec<&str>>();
        let first_index = indexes[0].parse::<usize>().unwrap();

        for i in 0..=first_index {
            // debug!("Array search index: {}", i);
            let replacement = format!(".{}", i);
            let re = Regex::new(r"\[\*\]").unwrap();
            let search_token = re.replace(mapping, &replacement).to_string();
            // debug!("Array search token: {}", search_token);

            if indexes.len() > 1 {
                results.extend(Transformer::array_search( tokens, &search_token, &indexes[1..].join("|")));
            } else {
                let val = Transformer::token_search( tokens, &search_token, false).unwrap_or_default();
                // debug!("Search iteration {} found {} results", i, val.len());
                if !val.is_empty() {
                    results.extend(val.into_iter().map(Some));
                } else {
                    results.push(None);
                }
            }

        }
        // debug!("Array search {} results", results.len());
        results
    }

    /// Get a single value from a mapping.
    /// 
    /// # Arguments
    /// 
    /// * `tokens` - A reference to a vector of tokens.
    /// * `mapping` - A reference to a JSONValue representing the mapping.
    /// * `key` - A reference to a string representing the key to search for.
    /// 
    /// # Returns
    /// 
    /// An optional JSONValue representing the search result.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let tokens = vec![("name".to_string(), JSONValue::String("John".to_string()))];
    /// let mapping = JSONValue::String("name".to_string());
    /// let key = "name".to_string();
    /// let result = Transformer::get_single_value(&tokens, &mapping, &key);
    /// assert_eq!(result.unwrap().unwrap(), JSONValue::String("John".to_string()));
    /// ```
    fn get_single_value(tokens: &[Token], mapping: &JSONValue, key: &String) -> Result<Option<JSONValue>, PyErr> {
    
        let mapping_item = match mapping.get(key) {
            Some(value) => {
                let value = value.as_str().unwrap();
                value
            }
            None => {
                let err_string = format!("{} mapping is required", &key);
                return Err(PyValueError::new_err(err_string));
            }
        };

    
        let val = match Transformer::token_search(tokens, mapping_item, true) {
            Some(val) => val,
            None => {
                return Ok(None);
            }
        };

        if val.is_empty() {
            return Ok(None);
        }

        Ok(Some(val[0].clone()))
    }
    
    /// Search for multiple values in an array.
    /// 
    /// # Arguments
    /// 
    /// * `tokens` - A reference to a vector of tokens.
    /// * `mapping` - A reference to a string representing the mapping to search for.
    /// * `pattern` - A reference to a string representing the maximum array indexes to search for.
    /// * `key` - A reference to a string representing the key to search for.
    /// 
    /// # Returns
    /// 
    /// A vector of optional JSONValues representing the search results.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let tokens = vec![("name".to_string(), JSONValue::String("John".to_string()))];
    /// let mapping = "name".to_string();
    /// let pattern = "0";
    /// let key = "name".to_string();
    /// let results = Transformer::array_search(&tokens, &mapping, &pattern, &key);
    /// assert_eq!(results.len(), 1);
    /// ```
    fn get_array_value(tokens: &[Token], mapping: &JSONValue , pattern: &str, key:&str) -> Result<Vec<Option<JSONValue>>, PyErr> {
        let mapping_item = match mapping.get(key) {
            Some(value) => {
              value
            }
            None => {
                return Ok(Vec::new())
            }
        };

        // is my mapping_item an array?
        if mapping_item.as_str().unwrap().contains("[*]") {
            return Ok(Transformer::array_search(tokens, mapping_item.as_str().unwrap(), pattern));
        }

        // if not do a normal search and return the results as a vector
        let value = Transformer::get_single_value(tokens, mapping, &key.to_string()).unwrap();
        let result: Vec<Option<JSONValue>> = vec![value];
        Ok(result)
    }

    /// Custom function to get metadata from a mapping.
    /// 
    /// Metadata is a nested object which accepts any key-value pair.
    /// 
    /// # Arguments
    /// 
    /// * `tokens` - A reference to a vector of tokens.
    /// * `mapping` - A reference to a JSONValue representing the mapping.
    /// 
    /// # Returns
    /// 
    /// An optional JSONValue representing the metadata object.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let tokens = vec![("name".to_string(), JSONValue::String("John".to_string()))];
    /// let mapping = JSONValue::Object(Map::new());
    /// let metadata = Transformer::get_metadata(&tokens, &mapping);
    /// assert_eq!(metadata.unwrap().unwrap(), JSONValue::Object(Map::new()));
    /// ```
    fn get_metadata(tokens: &[Token], mapping: &JSONValue) -> Result<Option<JSONValue>, PyErr> {
        let mapping_item = match mapping.get("metadata") {
            Some(value) => {
              value
            }
            None => {
                return Ok(None);
            }
        };
        let mut metadata = JSONValue::Object(Map::new());

        for (key, _) in mapping_item.as_object().unwrap() {
            // debug!("Processing metadata key: {}", key);
            let val = Transformer::get_single_value(tokens, mapping_item, &key.as_str().to_string()).unwrap();
            if let Some(val) = val {
                metadata.as_object_mut().unwrap().insert(key.clone(), val);
            }
        }

        Ok(Some(metadata))
    }

    /// Custom function to get metadata from a mapping.
    /// 
    /// Metadata is a nested object which accepts any key-value pair.
    /// 
    /// # Arguments
    /// 
    /// * `tokens` - A reference to a vector of tokens.
    /// * `mapping` - A reference to a JSONValue representing the mapping.
    /// * `pattern` - A reference to a string representing the maximum array indexes to search for.
    /// 
    /// # Returns
    /// 
    /// An optional JSONValue representing the metadata object. Each key contains an array of values.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let tokens = vec![("name".to_string(), JSONValue::String("John".to_string()))];
    /// let mapping = JSONValue::Object(Map::new());
    /// let pattern = "0";
    /// let metadata = Transformer::get_array_metadata(&tokens, &mapping, &pattern);
    /// assert_eq!(metadata.unwrap().unwrap(), JSONValue::Object(Map::new()));
    /// ```
    fn get_array_metadata(tokens: &[Token], mapping: &JSONValue, pattern: &str) -> Result<Option<JSONValue>, PyErr> {
        let mapping_item = match mapping.get("metadata") {
            Some(value) => {
              value
            }
            None => {
                return Ok(None);
            }
        };
        let mut metadata = JSONValue::Object(Map::new());

        for (key, _) in mapping_item.as_object().unwrap() {
            // debug!("Processing metadata key: {}", key);
            let val = Transformer::get_array_value(tokens, mapping_item, pattern, key.as_str()).unwrap();
            let converted_val: Vec<JSONValue> = val.iter().map(|v| if v.is_none() { JSONValue::Null } else { v.clone().unwrap() }).collect();
            metadata.as_object_mut().unwrap().insert(key.clone(), JSONValue::Array(converted_val));
        }

        Ok(Some(metadata))
    }

    /// Process a list of tokens to find the maximum index values.
    /// 
    /// This provides the maximum possible index for each array in a mapping.
    /// 
    /// # Arguments
    /// 
    /// * `matches` - A vector of strings representing the matches.
    /// 
    /// # Returns
    /// 
    /// A string representing the maximum index values.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let matches = vec!["$.name[0]", "$.name[1]"];
    /// let max_indexes = Transformer::get_max_indexes(matches);
    /// assert_eq!(max_indexes, "1");
    /// ```
    fn get_max_indexes(matches: &[Token]) -> String {
        // Compile the regex to find digits within square brackets
        let re = Regex::new(r"\d+").unwrap();

        // Convert the matches into a vector of vector of strings (similar to match_indexes in Python)
        let match_indexes: Vec<Vec<String>> = matches
            .iter()
            .map(|m| {
                re.captures_iter(&m.0)
                    .map(|cap| cap[0].to_string())
                    .collect()
            })
            .collect();

        // Initialize max_indexes to store the maximum index values
        let mut max_indexes: Vec<String> = Vec::new();

        // Iterate over match_indexes to determine the maximum values
        for match_vec in match_indexes {
            for (i, index) in match_vec.iter().enumerate() {
                if i >= max_indexes.len() {
                    max_indexes.push(index.clone());
                } else if index > &max_indexes[i] {
                    max_indexes[i] = index.clone();
                }
            }
        }

        // Join the max_indexes with "|" to form the final pattern
        max_indexes.join("|")
    }

    /// Create an object to represent a paragraph of text.
    /// 
    /// # Arguments
    /// 
    /// * `data` - A vector of optional JSONValues representing the data.
    /// * `title` - A vector of optional JSONValues representing the title.
    /// * `metadata` - An optional JSONValue representing the metadata. The object contains arrays
    ///   of values for each key.
    /// 
    /// # Returns
    /// 
    /// A vector of JSONValues representing the content.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let data = vec![Some(JSONValue::String("John".to_string()))];
    /// let title = vec![Some(JSONValue::String("Name".to_string()))];
    /// let metadata = Some(JSONValue::Object(Map::new()));
    /// let content = Transformer::create_data_content(data, title, metadata);
    /// assert_eq!(content.len(), 1);
    /// ```
    fn create_data_content(data: Vec<Option<JSONValue>>, title: Vec<Option<JSONValue>>, metadata: Option<JSONValue>) -> Vec<JSONValue> {
        let mut content = Vec::new();
        
        for (i, item) in data.iter().enumerate() {
            if item.is_some() {
                // create a model object
                let mut model = JSONValue::Object(Map::new());

                // insert data
                let mut data = item.clone().unwrap();

                // check if data is a JSONValue::Array and if not convert it to an array
                if !data.is_array() {
                    data = JSONValue::Array(vec![data]);
                }

                model.as_object_mut().unwrap().insert("data".to_string(), data);
                // insert title
                if i < title.len() {
                    model.as_object_mut().unwrap().insert("title".to_string(), if title[i].is_none() { JSONValue::Null } else {title[i].clone().unwrap()});
                } else {
                    model.as_object_mut().unwrap().insert("title".to_string(), JSONValue::Null);
                }
                
                // insert metadata
                if metadata.is_some() {
                    for (key, value) in metadata.clone().unwrap().as_object().unwrap() {
                        match value.as_array() {
                            Some(value) => {
                                if i < value.len() {
                                    model.as_object_mut().unwrap().insert(key.clone(), value[i].clone());
                                } else {
                                    model.as_object_mut().unwrap().insert(key.clone(), JSONValue::Null);
                                }
                            }
                            None => {
                                continue;
                            }
                        };
                    }
                }
                content.push(model);
            }
        }
        content
    }

    /// Create an object to represent a table.
    /// 
    /// # Arguments
    /// 
    /// * `rows` - A vector of optional JSONValues representing the rows.
    /// * `columns` - A vector of optional JSONValues representing the columns.
    /// * `title` - A vector of optional JSONValues representing the title.
    /// * `metadata` - An optional JSONValue representing the metadata. The object contains arrays
    ///   of values for each key.
    /// 
    /// # Returns
    /// 
    /// A vector of JSONValues representing the content.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let rows = vec![Some(JSONValue::String("John".to_string()))];
    /// let columns = vec![Some(JSONValue::String("Name".to_string()))];
    /// let title = vec![Some(JSONValue::String("Name".to_string()))];
    /// let metadata = Some(JSONValue::Object(Map::new()));
    /// let content = Transformer::create_table_content(rows, columns, title, metadata);
    /// assert_eq!(content.len(), 1);
    /// ```
    fn create_table_content(rows: Vec<Option<JSONValue>>, columns: Vec<Option<JSONValue>>, title: Vec<Option<JSONValue>>, metadata: Option<JSONValue>) -> Vec<JSONValue> {
        let mut content = Vec::new();
        
        for (i, item) in rows.iter().enumerate() {
            if item.is_some() {

                // create a model object
                let mut model = JSONValue::Object(Map::new());

                // insert data
                model.as_object_mut().unwrap().insert("rows".to_string(), item.clone().unwrap());

                // insert title
                if i < title.len() {
                    model.as_object_mut().unwrap().insert("title".to_string(), if title[i].is_none() { JSONValue::Null } else {title[i].clone().unwrap()});
                } else {
                    model.as_object_mut().unwrap().insert("title".to_string(), JSONValue::Null);
                }

                // insert columns
                if i < columns.len() {
                    model.as_object_mut().unwrap().insert("columns".to_string(), if columns[i].is_none() { JSONValue::Null } else {columns[i].clone().unwrap()});
                } else {
                    model.as_object_mut().unwrap().insert("columns".to_string(), JSONValue::Null);
                }
                
                // insert metadata
                if metadata.is_some() {
                    for (key, value) in metadata.clone().unwrap().as_object().unwrap() {
                        match value.as_array() {
                            Some(value) => {
                                if i < value.len() {
                                    model.as_object_mut().unwrap().insert(key.clone(), value[i].clone());
                                } else {
                                    model.as_object_mut().unwrap().insert(key.clone(), JSONValue::Null);
                                }
                            }
                            None => {
                                continue;
                            }
                        };
                    }
                }
                content.push(model);
            }
        }
        content
    }

    /// Create content based on the data provided.
    /// 
    /// # Arguments
    /// 
    /// * `data` - A vector of optional JSONValues representing the data.
    /// * `title` - A vector of optional JSONValues representing the title.
    /// * `columns` - A vector of optional JSONValues representing the columns.
    /// * `rows` - A vector of optional JSONValues representing the rows.
    /// * `metadata` - An optional JSONValue representing the metadata. The object contains arrays
    ///   of values for each key.
    /// 
    /// # Returns
    /// 
    /// A vector of JSONValues representing the content.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let data = vec![Some(JSONValue::String("John".to_string()))];
    /// let title = vec![Some(JSONValue::String("Name".to_string()))];
    /// let metadata = Some(JSONValue::Object(Map::new()));
    /// let content = Transformer::create_content(data, title, metadata);
    /// assert_eq!(content.len(), 1);
    /// ```
    fn create_content(data: Vec<Option<JSONValue>>, title: Vec<Option<JSONValue>>, columns: Vec<Option<JSONValue>>, rows: Vec<Option<JSONValue>>, metadata: Option<JSONValue>) -> Vec<JSONValue> {
        
        if !data.is_empty() {
            return Transformer::create_data_content(data, title, metadata);
        }

        if !rows.is_empty() {
            return Transformer::create_table_content(rows, columns, title, metadata);
        }
        Vec::new()
    }

    /// Get content from a mapping.
    /// 
    /// # Arguments
    /// 
    /// * `tokens` - A reference to a vector of tokens.
    /// * `mapping` - A reference to a JSONValue representing the mapping.
    /// 
    /// # Returns
    /// 
    /// An optional vector of JSONValues representing the content.
    fn get_content(tokens: &[Token], mapping: &JSONValue) -> Result<Option<Vec<JSONValue>>, PyErr> {
        let mapping_item = match mapping.get("content") {
            Some(value) => {
              value
            }
            None => {
                return Ok(None);
            }
        };
        let mut content = Vec::new();
        let mapping_arr = match mapping_item.as_array() {
            Some(value) => {
                value
            }
            None => {
                return Err(PyValueError::new_err("'content' mapping must be an array"));
            }
        };

        for mapping in mapping_arr {
            let matches = Transformer::get_all_tokens(tokens, mapping);

            if matches.is_empty() {
                continue;
            }

            let array_pattern = Transformer::get_max_indexes(&matches);
            let data = Transformer::get_array_value(&matches, mapping, &array_pattern, "data").unwrap();
            let title = Transformer::get_array_value(&matches, mapping, &array_pattern, "title").unwrap();
            let columns = Transformer::get_array_value(&matches, mapping, &array_pattern, "columns").unwrap();
            let rows = Transformer::get_array_value(&matches, mapping, &array_pattern, "rows").unwrap();
            let metadata = Transformer::get_array_metadata(&matches, mapping, &array_pattern).unwrap();

            content.extend(Transformer::create_content(data, title, columns, rows, metadata));

        }
        Ok(Some(content))
    }

    /// Transform a document based on a mapping.
    /// 
    /// # Arguments
    /// 
    /// * `mapping` - A reference to a JSONValue representing the mapping.
    /// * `tokens` - A reference to a vector of tokens.
    /// 
    /// # Returns
    /// 
    /// A JSONValue representing the transformed document.
    /// 
    /// # Examples
    /// 
    /// ```
    /// let mapping = JSONValue::Object(Map::new());
    /// let tokens = vec![("name".to_string(), JSONValue::String("John".to_string()))];
    /// let document = Transformer::transform(&mapping, &tokens);
    /// assert_eq!(document.id, "John");
    /// ```
    fn transform(mapping: &JSONValue, tokens: &[Token]) -> JSONValue {
        // let mut document = DocumentModel::default();
        let mut document = JSONValue::Object(Map::new());
        

        // set id
        let id = Transformer::get_single_value(tokens, mapping, &"id".to_string()).unwrap();
        if let Some(id) = id {
            
            document.as_object_mut().unwrap().insert("id".to_string(), id);
        }

        // set name
        let name = Transformer::get_single_value(tokens, mapping, &"name".to_string()).unwrap();
        if let Some(name) = name {
            
            document.as_object_mut().unwrap().insert("name".to_string(), name);
        }

        // set metadata
        let metadata = Transformer::get_metadata(tokens, mapping).unwrap();
        if let Some(metadata) = metadata {
            
            document.as_object_mut().unwrap().insert("metadata".to_string(), metadata);
        }

        // set content
        let content = Transformer::get_content(tokens, mapping).unwrap();
        if let Some(content) = content {
            
            document.as_object_mut().unwrap().insert("content".to_string(), JSONValue::Array(content));
            
        }
 
        document
    }

    fn transform_documents(mapping: &JSONValue, documents: &Vec<Vec<Token>>) -> Vec<JSONValue> {
        
        debug!("Transforming documents...");
        documents.par_iter().map(|document| {
            Transformer::transform(mapping, document)
        }).collect()

    }

}

/// Python implementation of the Transformer class
#[pyclass(module="cleansweep_core._cleansweep_core", name="Transformer")]
pub struct PyTransformer;

#[pymethods]
impl PyTransformer {
    #[new]
    fn new() -> Self {
        PyTransformer
    }

    #[pyo3(signature = (mapping_path, document_path, root=None))]
    pub fn transform_document(&self, py: Python, mapping_path: String, document_path: String, root: Option<String>) -> PyResult<Vec<PyObject>> {
        let mapping: JSONValue = read_to_serde_value(&mapping_path);

        let tokenized_documents = Tokenizer::tokenize_document(&document_path, &root).unwrap();
        debug!("Documents tokenized: {:?}", tokenized_documents.len());

        let res = Transformer::transform_documents(&mapping, &tokenized_documents.to_vec());
        let py_res: Vec<PyObject> = res.iter().map(|r| serde_value_to_pyobject(py, r)).collect();
        debug!("Documents transformed: {:?}", py_res.len());
        Ok(py_res)
    }
}
