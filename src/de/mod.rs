pub mod data;

use nebula_fbthrift_graph_v2::ExecutionResponse;
use serde::Deserialize;
use crate::de::data::{DataDeserializeError, DataDeserializer};

pub fn deserialize_execution_response<'de, D: Deserialize<'de>>(
    execution_response: &'de ExecutionResponse,
) -> Result<Vec<D>, DataDeserializeError> {
    let mut data_set: Vec<D> = vec![];

    let names = match &execution_response.data {
        Some(dat) => {
            let colnms = dat.clone().column_names.iter()
            .map(|x| String::from_utf8(x.to_vec())
            .unwrap_or("nil".to_string()))
            .collect::<Vec<String>>();
            colnms.to_owned()
        },
        None => Vec::new()
    };

    let rows = match &execution_response.data {
        Some(dat) => {
            let rows = dat.clone().rows.to_owned();
            rows
        },
        None => Vec::new()
    };

    let mut row_iter = rows.into_iter();

    loop {
        match row_iter.next() {
            Some(row) => {
                let mut data_deserializer = DataDeserializer::new(&names,&row.values);
                let data = D::deserialize(&mut data_deserializer)?;
                data_set.push(data);
            },
            None => {
                break;
            }
        }
    }

    Ok(data_set)
}