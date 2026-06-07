mod client;
mod error;
mod frames;
mod row;
mod scalar;
mod state;

pub use client::Client;
pub use error::Error;
pub use row::KustoRow;
pub use scalar::KustoScalar;

pub use kusto_stream_macros::KustoRow;

pub use azure_core::Uuid as KustoGuid;
pub use serde_json::Value as KustoDynamic;
pub type KustoDateTime = chrono::DateTime<chrono::Utc>;
