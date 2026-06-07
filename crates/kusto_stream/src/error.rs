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

    #[error("Row Type Error: {reason}")]
    RowType { reason: String },
}
