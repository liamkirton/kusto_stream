use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Azure Error: {0}")]
    Azure(#[from] azure_core::Error),

    #[error("Credential Error")]
    Credential,

    #[error("Invalid Cluster: {cluster}")]
    InvalidCluster { cluster: String },

    #[error("Http Protocol Error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Json Deserialisation Error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Response Stream Parsing Error")]
    ResponseStreamParsing,

    #[error("Kusto Protocol Error")]
    KustoProtocol,

    #[error("Kusto Protocol Error")]
    KustoError,

    #[error("Row Type Error: {reason}")]
    RowType { reason: String },

    #[error("{error}")]
    General { error: String },
}

pub(crate) fn parse_kusto_error(error: &serde_json::Value) -> String {
    if let Some(error) = error.get("error") {
        let mut msg: String = String::new();

        if let Some(v) = error.get("@message") {
            msg += &v.to_string();
        } else if let Some(v) = error.get("message") {
            msg += &v.to_string();
        }

        if let Some(inner_error) = error.get("innererror") {
            msg += " [";
            if let Some(v) = inner_error.get("@message") {
                msg += &v.to_string();
            } else if let Some(v) = inner_error.get("message") {
                msg += &v.to_string();
            }
            msg += "]";
        }

        msg
    } else {
        "Unknown".to_owned()
    }
}
