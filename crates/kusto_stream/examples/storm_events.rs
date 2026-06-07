//! Streams the public `StormEvents` sample table from the `help` cluster into a
//! statically-typed row struct.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example storm_events
//! ```
//!
//! Requires Azure auth (via `DeveloperToolsCredential`) and network access to
//! `help.kusto.windows.net`.

use anyhow::Result;
use azure_identity::DeveloperToolsCredential;
use serde::Deserialize;
use tracing::info;
use tracing_subscriber::EnvFilter;

use kusto_stream::{Client, KustoDateTime, KustoDynamic, KustoRow};

#[allow(dead_code, non_snake_case)]
#[derive(Debug, Deserialize, KustoRow)]
struct Row {
    StartTime: KustoDateTime,
    EndTime: KustoDateTime,
    EpisodeId: i32,
    EventId: i32,
    State: String,
    EventType: String,
    InjuriesDirect: i32,
    InjuriesIndirect: i32,
    DeathsDirect: i32,
    DeathsIndirect: i32,
    DamageProperty: i32,
    DamageCrops: i32,
    Source: String,
    BeginLocation: String,
    EndLocation: String,
    BeginLat: Option<f64>,
    BeginLon: Option<f64>,
    EndLat: Option<f64>,
    EndLon: Option<f64>,
    EpisodeNarrative: String,
    EventNarrative: String,
    StormSummary: KustoDynamic,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,kusto_stream=debug")),
        )
        .init();

    let cluster = "help.kusto.windows.net";
    let db = "Samples";
    let kql = "StormEvents | take 1";

    info!("Querying '{}/{}'...", cluster, db);

    let mut client = Client::new(cluster, Some(DeveloperToolsCredential::new(None)?))?;

    let mut row_count = 0;
    client
        .query::<Row, _>(db, kql, &mut |rows: Vec<Row>| {
            row_count += rows.len();
        })
        .await?;

    info!("Query Complete! Got {} Rows", row_count);

    Ok(())
}
