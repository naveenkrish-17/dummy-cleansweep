use log::{debug, warn, error};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use md5::{Md5, Digest};
use std::convert::Infallible;
use std::fmt::Write; // For formatting the hash as a string
use std::fmt;

mod common;
mod tokenize;
mod transform;

#[derive(serde::Deserialize)]
struct ConsolidatedQuestion {
    #[serde(default)]
    source_ids: Vec<String>,
    sufficient_ids: Option<Vec<String>>,
    question: String,
    answer: String,
}

impl ConsolidatedQuestion {
    /// Generates a unique question ID by combining the question and answer fields,
    /// creating an MD5 hash of the combined string, and converting the hash result
    /// into a hexadecimal string.
    ///
    /// # Returns
    ///
    /// A `String` representing the MD5 hash of the combined question and answer.
    ///
    /// # Panics
    ///
    /// This function will panic if it is unable to write to the string during the
    /// conversion of the hash result to a hexadecimal string.
    fn question_id(&self) -> String {
        // Combine the question and answer into a single string
        let combined = format!("question: {}|answer: {}", self.question, self.answer);

        // Create an MD5 hash of the combined string
        let mut hasher = Md5::new();
        hasher.update(combined.as_bytes());

        // Convert the hash result into a hexadecimal string
        let result = hasher.finalize();
        let mut hash_string = String::new();
        for byte in result {
            write!(&mut hash_string, "{:02x}", byte).expect("Unable to write to string");
        }

        hash_string
    }


    /// Creates a UUID for the question.
    ///
    /// # Returns
    ///
    /// A `String` representing the UUID of the question.
    fn question_uuid(&self) -> String {
        // Create a UUID for the question
        let uuid = uuid::Uuid::new_v4();
        uuid.to_string()
    }

}

#[derive(serde::Deserialize)]
struct ConsolidatedResponse {
    items: Vec<ConsolidatedQuestion>
}

#[derive(Clone)]
struct Question {
    question_id: String,
    question_uuid: String,
    question: String,
    answer: String,
    source_id: String,
    cluster_id: String,
    is_sufficient: bool, 
    metadata_language: String
}



impl<'a, 'py> IntoPyObject<'py> for &'a Question {
    type Target = PyDict;
    type Output = Bound<'py, Self::Target>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);
        
        dict.set_item("question_id", &self.question_id).unwrap();
        dict.set_item("question_uuid", &self.question_uuid).unwrap();
        dict.set_item("question", &self.question).unwrap();
        dict.set_item("answer", &self.answer).unwrap();
        dict.set_item("source_id", &self.source_id).unwrap();
        dict.set_item("cluster_id", &self.cluster_id).unwrap();
        dict.set_item("is_sufficient", self.is_sufficient).unwrap();
        dict.set_item("metadata_language", &self.metadata_language).unwrap();

        Ok(dict)
    }
}


impl fmt::Debug for Question {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Question")
            .field("question_id", &self.question_id)
            .field("question_uuid", &self.question_uuid)
            .field("question", &self.question)
            .field("answer", &self.answer)
            .field("source_id", &self.source_id)
            .field("cluster_id", &self.cluster_id)
            .field("is_sufficient", &self.is_sufficient)
            .field("metadata_language", &self.metadata_language)
            .finish()
    }
}

impl fmt::Display for Question {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Question: {} (ID: {} UUID {})\nAnswer: {}\nSource ID: {}\n Is Sufficient: {}\nCluster ID: {}\nLanguage: {}", self.question, self.question_id, self.question_uuid, self.answer, self.source_id, self.is_sufficient, self.cluster_id, self.metadata_language)
    }
}

/// @parameters
/// results: list[str | None] - serialised JSON response from OpenAI Chat API
/// frame_recors: list[list[dict]] - list of list of dictionaries containing question_id, question, answer, source_id
/// cluster_ids: list[int] - list of cluster ids
#[pyfunction]
#[pyo3(signature = (results, frame_records, cluster_ids))]
fn process_merge_results(py: Python, results: &'_ Bound<'_, PyList>, frame_records: &'_ Bound<'_, PyList>, cluster_ids: Vec<String>) -> PyResult<Py<PyList>> {
    // init_logger();
    
    // convert inputs to rust types
    let results: Vec<Option<String>> = results.iter().map(|x| x.extract().unwrap_or_default()).collect();

    let frame_records: Vec<Vec<Question>> = frame_records.iter().map(|frame| {
        let list = frame.downcast::<PyList>().unwrap();
        let frame_list: Vec<Question> = list.iter().map(|record| {

            let question_id: String = record.get_item("question_id").map(|x| x.extract().unwrap_or_default()).unwrap_or_default();
            let question_uuid: String = record.get_item("question_uuid").map(|x| x.extract().unwrap_or_default()).unwrap_or_default();
            let question: String = record.get_item("question").map(|x| x.extract().unwrap_or_default()).unwrap_or_default();
            let answer: String = record.get_item("answer").map(|x| x.extract().unwrap_or_default()).unwrap_or_default();
            let source_id: String = record.get_item("source_id").map(|x| x.extract().unwrap_or_default()).unwrap_or_default();
            let cluster_id: String = record.get_item("cluster_id").map(|x| x.extract().unwrap_or_default()).unwrap_or_default();
            let metadata_language: String = record.get_item("metadata_language").map(|x| x.extract().unwrap_or_default()).unwrap_or_default();
                Question {
                    question_id,
                    question_uuid,
                    question,
                    answer,
                    source_id,
                    cluster_id,
                    is_sufficient: false,
                    metadata_language
                }
            }
        ).collect();
        frame_list
    }).collect();

    // iterate over results and frame records to merge the results
    let mut qa_objects: Vec<Question> = Vec::new();

    for ((cluster_id, result), frame) in cluster_ids.iter().zip(results.iter()).zip(frame_records.iter()) {

        if let Some(result) = result {
            
            // let result: Result<Value, serde_json::Error> = serde_json::from_str(&result);
            let _result: Result<ConsolidatedResponse, serde_json::Error> = serde_json::from_str(result);
            if let Err(e) = _result {
                error!("Error deserialising result: {} ({})", e, result);
                continue;
            }
            if let Ok(deserialised_result) = _result {
                // let mut qa_objects_cluster: Vec<Question> = Vec::new();
                for consolidated_question in deserialised_result.items.iter() {
                    for source_id in &consolidated_question.source_ids {
                        let source_question = frame.iter().find(|x| x.question_id == *source_id);
                        if let Some(source_question) = source_question {
                            let sufficient_ids = match consolidated_question.sufficient_ids.clone()  {
                                Some(ids) => ids,
                                None => consolidated_question.source_ids.clone()
                            };
                            let is_sufficient = sufficient_ids.contains(source_id);

                            qa_objects.push(Question {
                                question_id: consolidated_question.question_id(),
                                question_uuid: consolidated_question.question_uuid(),
                                question: consolidated_question.question.clone(),
                                answer: consolidated_question.answer.clone(),
                                source_id: source_question.source_id.clone(),
                                is_sufficient,
                                metadata_language: source_question.metadata_language.clone(),
                                cluster_id: cluster_id.clone(),
                            });
                        } else {
                            let source_question = frame.iter().find(|x| x.question_id == consolidated_question.question_id());
                            if let Some(source_question) = source_question {
                                qa_objects.push(Question {
                                    question_id: consolidated_question.question_id(),
                                    question_uuid: consolidated_question.question_uuid(),
                                    question: consolidated_question.question.clone(),
                                    answer: consolidated_question.answer.clone(),
                                    source_id: source_question.source_id.clone(),
                                    is_sufficient: true,
                                    metadata_language: source_question.metadata_language.clone(),
                                    cluster_id: cluster_id.clone(),
                                });
                            } else {
                                warn!("No matching question found for question_id: {}", consolidated_question.question_id());
                            }
                        }
                    }
                }
                // qa_objects.push(qa_objects_cluster);
            }

        } else {
            qa_objects.extend(frame.clone());
        }
        

    }

    debug!("Created {} questions", qa_objects.len());

    // Convert the Rust vector to a Python list
    let py_list = PyList::new(py, &qa_objects).unwrap();

    // Return the Python list
    Ok(py_list.into())
}

#[pymodule]
fn _cleansweep_core(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    let _ = m.add_function(wrap_pyfunction!(process_merge_results, m)?);
    m.add_class::<tokenize::tokenizer::PyTokenizer>()?;
    m.add_class::<transform::transformer::PyTransformer>()?;
    Ok(())
}