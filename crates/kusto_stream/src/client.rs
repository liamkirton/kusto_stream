use std::{
    fmt::Debug,
    sync::{Arc, LazyLock},
};

use azure_core::{
    credentials::{AccessToken, TokenCredential},
    time::{Duration, OffsetDateTime},
};
use azure_identity::DeveloperToolsCredential;
use bytes::{BufMut, BytesMut};
use memchr::memchr;
use regex::Regex;
use reqwest::Response;
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, error, info};

use crate::{
    KustoRow,
    error::{Error, parse_kusto_error},
    frames::KustoResponseV2,
    state::KustoResponseState,
};

const CLUSTER_SUFFIX: &str = ".kusto.windows.net";

static CLUSTER_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)?$").unwrap()
});

pub struct Client {
    cluster: String,
    credential: Option<Arc<dyn TokenCredential>>,
    token: Option<Arc<AccessToken>>,
}

pub struct QueryResult {
    pub success: bool,
    pub error: Option<String>,

    pub bytes: usize,
    pub rows: usize,
}

struct ResponseParser {
    state: KustoResponseState,
    error: Option<String>,
    bytes: usize,
    rows: usize,
    frame_accumulator: BytesMut,
    frame_accumulator_scanned: usize,
}

impl Client {
    pub fn new(cluster: &str, credential: Option<Arc<dyn TokenCredential>>) -> Result<Self, Error> {
        let cluster_base = cluster.strip_suffix(CLUSTER_SUFFIX).unwrap_or(cluster);

        if !CLUSTER_RE.is_match(cluster_base) {
            return Err(Error::InvalidCluster {
                cluster: cluster.to_owned(),
            });
        }

        Ok(Client {
            cluster: format!("{cluster_base}{CLUSTER_SUFFIX}"),
            credential,
            token: None,
        })
    }

    pub async fn query<R, C>(
        &mut self,
        db: &str,
        kql: &str,
        row_handler: &mut C,
    ) -> Result<QueryResult, Error>
    where
        R: Debug + for<'a> Deserialize<'a> + KustoRow,
        C: FnMut(Vec<R>),
    {
        let request_client = reqwest::Client::builder()
            .brotli(true)
            .deflate(false)
            .gzip(true)
            .build()?;

        let token = self.get_token().await?;

        info!("Querying '{}'...", self.cluster);

        let mut response = request_client
            .post(format!("https://{}/v2/rest/query", self.cluster))
            .header("Accept", "application/json")
            .header("Authorization", format!("Bearer {}", token.token.secret()))
            .json(&json!({
                "properties": {
                    "Options": {
                        "norequesttimeout": true,
                        "notruncation": true,
                        "results_progressive_enabled": true,
                        "results_v2_fragment_primary_tables": true,
                        "results_v2_newlines_between_frames": true,
                    }
                },
                "db": db,
                "csl": kql
            }))
            .send()
            .await?;

        let status = response.status();

        if status.is_success() {
            self.parse_response::<R, C>(&mut response, row_handler)
                .await
        } else {
            let response_text = &response.text().await?;
            let error = match serde_json::from_str(response_text) {
                Ok(error_json) => Error::General {
                    error: parse_kusto_error(&error_json),
                },
                Err(_) => Error::General {
                    error: format!("Unexpected Error Response: {response_text}"),
                },
            };
            error!("Response: {}", status);
            error!("Error: {}", error);
            Err(error)
        }
    }

    async fn get_token(&mut self) -> Result<Arc<AccessToken>, Error> {
        let should_refresh = match &self.token {
            Some(token) => token.expires_on < OffsetDateTime::now_utc() + Duration::minutes(5),
            None => true,
        };

        if should_refresh {
            if self.credential.is_none() {
                debug!("Creating New 'DeveloperToolsCredential'");
                self.credential = Some(DeveloperToolsCredential::new(None)?);
            }

            let credential: Arc<dyn TokenCredential> = match &self.credential {
                Some(v) => v.clone(),
                None => return Err(Error::Credential),
            };

            let kusto_scope = format!("https://{}/.default", self.cluster);
            debug!("Requesting New Token For Scope '{}'...", kusto_scope);
            self.token = Some(Arc::new(
                credential.get_token(&[kusto_scope.as_str()], None).await?,
            ));
        } else {
            debug!("Replaying Cached Token");
        }

        match &self.token {
            Some(t) => Ok(t.clone()),
            None => Err(Error::Credential),
        }
    }

    async fn parse_response<R, C>(
        &self,
        response: &mut Response,
        row_handler: &mut C,
    ) -> Result<QueryResult, Error>
    where
        R: Debug + for<'a> Deserialize<'a> + KustoRow,
        C: FnMut(Vec<R>),
    {
        let mut response_parser = ResponseParser::new();
        while let Some(response_chunk) = response.chunk().await? {
            response_parser.process_chunk(&response_chunk, row_handler)?;
        }

        let success = response_parser.process_tail()?;

        Ok(QueryResult {
            success,
            error: response_parser.error,
            bytes: response_parser.bytes,
            rows: response_parser.rows,
        })
    }
}

impl ResponseParser {
    fn new() -> Self {
        Self {
            state: KustoResponseState::New,
            error: None,
            bytes: 0,
            rows: 0,
            frame_accumulator: BytesMut::new(),
            frame_accumulator_scanned: 0,
        }
    }

    fn process_chunk<R, C>(&mut self, chunk: &[u8], row_handler: &mut C) -> Result<(), Error>
    where
        R: Debug + for<'a> Deserialize<'a> + KustoRow,
        C: FnMut(Vec<R>),
    {
        self.bytes += chunk.len();
        self.frame_accumulator.put_slice(chunk);

        // Rely on 'results_v2_newlines_between_frames' behaviour: "... }\n,{ ..."
        while let Some(frame_end) = memchr(
            b'\n',
            &self.frame_accumulator[self.frame_accumulator_scanned..],
        ) && let Some(frame_start) = memchr(b'{', &self.frame_accumulator)
        {
            let frame_end = self.frame_accumulator_scanned + frame_end;

            if frame_start >= frame_end
                || frame_end - frame_start < 2
                || frame_start >= 2
                || self.frame_accumulator[frame_start] != b'{'
                || self.frame_accumulator[frame_end - 1] != b'}'
            {
                return Err(Error::ResponseStreamParsing);
            }

            let frame: KustoResponseV2<R> =
                serde_json::from_slice(&self.frame_accumulator[frame_start..frame_end])?;
            self.process_frame(frame, row_handler)?;

            self.frame_accumulator = self.frame_accumulator.split_off(frame_end + 1);
            self.frame_accumulator_scanned = 0;
        }

        self.frame_accumulator_scanned = self.frame_accumulator.len();
        Ok(())
    }

    fn process_frame<R, C>(
        &mut self,
        frame: KustoResponseV2<R>,
        row_handler: &mut C,
    ) -> Result<(), Error>
    where
        R: Debug + for<'a> Deserialize<'a> + KustoRow,
        C: FnMut(Vec<R>),
    {
        match frame {
            KustoResponseV2::DataSetHeader(header) => {
                if self.state != KustoResponseState::New
                    || header.version != "v2.0"
                    || !header.is_progressive
                {
                    debug!("Unexpected DataSetHeader");
                    return Err(Error::KustoProtocol);
                }
                self.state = KustoResponseState::RequireResponseSchema;
            }
            KustoResponseV2::DataTable(table) => {
                if self.state != KustoResponseState::RequireResponseSchema
                    || table.table_kind == "PrimaryResult"
                    || table.table_name == "PrimaryResult"
                {
                    debug!("Unexpected Non-Progressive PrimaryResult Table");
                    return Err(Error::KustoProtocol);
                }
            }
            KustoResponseV2::TableHeader(header) => {
                if self.state != KustoResponseState::RequireResponseSchema
                    || header.table_kind != "PrimaryResult"
                {
                    debug!(
                        "Unexpected Progressive Non-PrimaryResult Table: {}/{}",
                        header.table_kind, header.table_name
                    );
                    return Err(Error::KustoProtocol);
                }

                let validate_columns: Vec<(&str, &str)> = header
                    .columns
                    .iter()
                    .map(|c| (c.column_name.as_str(), c.column_type.as_str()))
                    .collect();
                R::validate(&validate_columns)?;

                self.state = KustoResponseState::Streaming;
            }
            KustoResponseV2::TableFragment(fragment) => {
                if self.state != KustoResponseState::Streaming {
                    debug!("Unexpected TableFragment");
                    return Err(Error::KustoProtocol);
                }
                if !fragment.rows.is_empty() {
                    self.rows += fragment.rows.len();
                    row_handler(fragment.rows)
                }
            }
            KustoResponseV2::TableProgress(progress) => {
                if self.state != KustoResponseState::Streaming {
                    debug!("Unexpected TableProgress");
                    return Err(Error::KustoProtocol);
                }
                debug!("Table Progress: {}%", progress.table_progress);
            }
            KustoResponseV2::TableCompletion(completion) => {
                if self.state != KustoResponseState::Streaming || completion.row_count != self.rows
                {
                    debug!("Unexpected TableCompletion");
                    return Err(Error::KustoProtocol);
                }
                debug!("Table Complete: {} Rows", completion.row_count);
            }
            KustoResponseV2::DataSetCompletion(completion) => {
                self.state = if !completion.has_errors && !completion.cancelled {
                    debug!("Query Execution Complete");
                    KustoResponseState::Complete
                } else {
                    debug!("Query Execution With Errors");
                    if let Some(errors) = completion.one_api_errors
                        && let Some(error) = errors.first()
                    {
                        self.error = Some(parse_kusto_error(error));
                    }
                    KustoResponseState::CompleteWithErrors
                }
            }
        }

        Ok(())
    }

    fn process_tail(&self) -> Result<bool, Error> {
        if memchr(b'{', &self.frame_accumulator).is_some() {
            return Err(Error::ResponseStreamParsing);
        }

        Ok(self.state == KustoResponseState::Complete)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::{Value, json};

    use super::{Client, ResponseParser};
    use crate::{Error, KustoDateTime, KustoDynamic, KustoGuid, KustoRow};

    #[derive(Debug, PartialEq, Deserialize)]
    struct TestRow {
        a: i64,
        b: String,
    }

    impl KustoRow for TestRow {
        fn validate(columns: &[(&str, &str)]) -> Result<(), Error> {
            let expected = [("a", "long"), ("b", "string")];
            if columns.len() != expected.len() {
                return Err(Error::RowType {
                    reason: format!("expected {} columns, got {}", expected.len(), columns.len()),
                });
            }
            for (got, want) in columns.iter().zip(expected.iter()) {
                if got != want {
                    return Err(Error::RowType {
                        reason: format!("expected {want:?}, got {got:?}"),
                    });
                }
            }
            Ok(())
        }
    }

    fn happy_frames() -> Vec<Value> {
        vec![
            json!({"FrameType": "DataSetHeader", "IsProgressive": true, "Version": "v2.0"}),
            json!({
                "FrameType": "TableHeader", "TableId": 0,
                "TableKind": "PrimaryResult", "TableName": "PrimaryResult",
                "Columns": [
                    {"ColumnName": "a", "ColumnType": "long"},
                    {"ColumnName": "b", "ColumnType": "string"},
                ],
            }),
            json!({
                "FrameType": "TableFragment", "TableId": 0, "FieldCount": 2,
                "TableFragmentType": "DataAppend", "Rows": [[1, "x"], [2, "y"]],
            }),
            json!({"FrameType": "TableCompletion", "TableId": 0, "RowCount": 2}),
            json!({"FrameType": "DataSetCompletion", "HasErrors": false, "Cancelled": false}),
        ]
    }

    fn build_stream(frames: &[Value]) -> Vec<u8> {
        let mut out = vec![b'['];
        for (i, frame) in frames.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(frame.to_string().as_bytes());
            out.push(b'\n');
        }
        out.push(b']');
        out
    }

    fn run_response_parser_loop<R>(chunks: &[&[u8]]) -> (Result<bool, Error>, Vec<R>)
    where
        R: std::fmt::Debug + for<'a> Deserialize<'a> + KustoRow,
    {
        let mut rows = Vec::new();
        let mut sink = |batch: Vec<R>| rows.extend(batch);

        let mut parser = ResponseParser::new();
        for chunk in chunks {
            if let Err(e) = parser.process_chunk::<R, _>(chunk, &mut sink) {
                return (Err(e), rows);
            }
        }
        (parser.process_tail(), rows)
    }

    #[test]
    fn test_cluster_names_are_normalised() {
        assert_eq!(
            Client::new("foo", None).unwrap().cluster,
            "foo.kusto.windows.net",
        );
        assert_eq!(
            Client::new("foo.eastus2", None).unwrap().cluster,
            "foo.eastus2.kusto.windows.net",
        );
        assert_eq!(
            Client::new("help.kusto.windows.net", None).unwrap().cluster,
            "help.kusto.windows.net",
        );
    }

    #[test]
    fn test_cluster_names_are_validated() {
        for bad in [
            "foo/bar",
            "foo:8080",
            "https://foo",
            "FOO",
            "",
            "a.b.c",
            "foo.",
        ] {
            assert!(
                matches!(Client::new(bad, None), Err(Error::InvalidCluster { .. })),
                "should reject {bad:?}",
            );
        }
    }

    #[test]
    fn test_datetime_deserialises_from_string() {
        let dt: KustoDateTime = serde_json::from_str("\"2007-09-29T08:11:00Z\"").unwrap();
        assert_eq!(dt.to_rfc3339(), "2007-09-29T08:11:00+00:00");
    }

    #[test]
    fn test_guid_deserialises_from_string() {
        let guid: KustoGuid =
            serde_json::from_str("\"936da01f-9abd-4d9d-80c7-02af85c822a8\"").unwrap();
        assert_eq!(guid.to_string(), "936da01f-9abd-4d9d-80c7-02af85c822a8");
    }

    #[test]
    fn test_parses_well_formed_response() {
        let stream = build_stream(&happy_frames());
        let (result, rows) = run_response_parser_loop::<TestRow>(&[&stream]);

        assert!(result.unwrap(), "should reach Complete");
        assert_eq!(
            rows,
            vec![
                TestRow {
                    a: 1,
                    b: "x".into()
                },
                TestRow {
                    a: 2,
                    b: "y".into()
                },
            ],
        );
    }

    #[test]
    fn test_handles_arbitrary_chunk_boundaries() {
        let stream = build_stream(&happy_frames());
        for split in 0..=stream.len() {
            let (head, tail) = stream.split_at(split);
            let (result, rows) = run_response_parser_loop::<TestRow>(&[head, tail]);

            let complete = result.unwrap_or_else(|e| panic!("split {split}: process error: {e}"));
            assert!(complete, "split {split}: did not reach Complete");
            assert_eq!(rows.len(), 2, "split {split}: wrong row count");
        }
    }

    #[test]
    fn test_incomplete_stream_is_not_complete() {
        let frames = &happy_frames()[..4];
        let stream = build_stream(frames);
        let (result, rows) = run_response_parser_loop::<TestRow>(&[&stream]);

        assert!(
            !result.unwrap(),
            "should not be Complete without DataSetCompletion"
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_rejects_schema_mismatch() {
        let mut frames = happy_frames();
        frames[1] = json!({
            "FrameType": "TableHeader", "TableId": 0,
            "TableKind": "PrimaryResult", "TableName": "PrimaryResult",
            "Columns": [
                {"ColumnName": "a", "ColumnType": "int"},
                {"ColumnName": "b", "ColumnType": "string"},
            ],
        });
        let stream = build_stream(&frames);
        let (result, _) = run_response_parser_loop::<TestRow>(&[&stream]);

        assert!(
            matches!(result, Err(Error::RowType { .. })),
            "got {result:?}"
        );
    }

    #[test]
    fn test_rejects_out_of_order_frames() {
        let frames = [
            json!({"FrameType": "DataSetHeader", "IsProgressive": true, "Version": "v2.0"}),
            json!({
                "FrameType": "TableFragment", "TableId": 0, "FieldCount": 2,
                "TableFragmentType": "DataAppend", "Rows": [[1, "x"]],
            }),
        ];
        let stream = build_stream(&frames);
        let (result, _) = run_response_parser_loop::<TestRow>(&[&stream]);

        assert!(
            matches!(result, Err(Error::KustoProtocol)),
            "got {result:?}"
        );
    }

    #[test]
    fn test_rejects_row_count_mismatch() {
        let mut frames = happy_frames();
        frames[3] = json!({"FrameType": "TableCompletion", "TableId": 0, "RowCount": 99});
        let stream = build_stream(&frames);
        let (result, _) = run_response_parser_loop::<TestRow>(&[&stream]);

        assert!(
            matches!(result, Err(Error::KustoProtocol)),
            "got {result:?}"
        );
    }

    #[test]
    fn test_rejects_non_progressive_dataset_header() {
        let mut frames = happy_frames();
        frames[0] =
            json!({"FrameType": "DataSetHeader", "IsProgressive": false, "Version": "v2.0"});
        let stream = build_stream(&frames);
        let (result, _) = run_response_parser_loop::<TestRow>(&[&stream]);

        assert!(
            matches!(result, Err(Error::KustoProtocol)),
            "got {result:?}"
        );
    }

    // A real progressive v2 response captured from `help.kusto.windows.net`
    // (`Samples` / `StormEvents | take 1`).
    const STORMEVENTS_RAW_RESPONSE: &[u8] = b"[{\"FrameType\":\"DataSetHeader\",\"IsProgressive\":true,\"Version\":\"v2.0\",\"IsFragmented\":true,\"ErrorReportingPlacement\":\"InData\"}\n,{\"FrameType\":\"DataTable\",\"TableId\":0,\"TableKind\":\"QueryProperties\",\"TableName\":\"@ExtendedProperties\",\"Columns\":[{\"ColumnName\":\"TableId\",\"ColumnType\":\"int\"},{\"ColumnName\":\"Key\",\"ColumnType\":\"string\"},{\"ColumnName\":\"Value\",\"ColumnType\":\"dynamic\"}],\"Rows\":[[1,\"Visualization\",\"{\\\"Visualization\\\":null,\\\"Title\\\":null,\\\"XColumn\\\":null,\\\"Series\\\":null,\\\"YColumns\\\":null,\\\"AnomalyColumns\\\":null,\\\"XTitle\\\":null,\\\"YTitle\\\":null,\\\"XAxis\\\":null,\\\"YAxis\\\":null,\\\"Legend\\\":null,\\\"YSplit\\\":null,\\\"Accumulate\\\":false,\\\"IsQuerySorted\\\":false,\\\"Kind\\\":null,\\\"Ymin\\\":\\\"NaN\\\",\\\"Ymax\\\":\\\"NaN\\\",\\\"Xmin\\\":null,\\\"Xmax\\\":null}\"]]}\n,{\"FrameType\":\"TableHeader\",\"TableId\":1,\"TableKind\":\"PrimaryResult\",\"TableName\":\"PrimaryResult\",\"Columns\":[{\"ColumnName\":\"StartTime\",\"ColumnType\":\"datetime\"},{\"ColumnName\":\"EndTime\",\"ColumnType\":\"datetime\"},{\"ColumnName\":\"EpisodeId\",\"ColumnType\":\"int\"},{\"ColumnName\":\"EventId\",\"ColumnType\":\"int\"},{\"ColumnName\":\"State\",\"ColumnType\":\"string\"},{\"ColumnName\":\"EventType\",\"ColumnType\":\"string\"},{\"ColumnName\":\"InjuriesDirect\",\"ColumnType\":\"int\"},{\"ColumnName\":\"InjuriesIndirect\",\"ColumnType\":\"int\"},{\"ColumnName\":\"DeathsDirect\",\"ColumnType\":\"int\"},{\"ColumnName\":\"DeathsIndirect\",\"ColumnType\":\"int\"},{\"ColumnName\":\"DamageProperty\",\"ColumnType\":\"int\"},{\"ColumnName\":\"DamageCrops\",\"ColumnType\":\"int\"},{\"ColumnName\":\"Source\",\"ColumnType\":\"string\"},{\"ColumnName\":\"BeginLocation\",\"ColumnType\":\"string\"},{\"ColumnName\":\"EndLocation\",\"ColumnType\":\"string\"},{\"ColumnName\":\"BeginLat\",\"ColumnType\":\"real\"},{\"ColumnName\":\"BeginLon\",\"ColumnType\":\"real\"},{\"ColumnName\":\"EndLat\",\"ColumnType\":\"real\"},{\"ColumnName\":\"EndLon\",\"ColumnType\":\"real\"},{\"ColumnName\":\"EpisodeNarrative\",\"ColumnType\":\"string\"},{\"ColumnName\":\"EventNarrative\",\"ColumnType\":\"string\"},{\"ColumnName\":\"StormSummary\",\"ColumnType\":\"dynamic\"}]}\n,{\"FrameType\":\"TableFragment\",\"TableFragmentType\":\"DataAppend\",\"TableId\":1,\"Rows\":[[\"2007-09-29T08:11:00Z\",\"2007-09-29T08:11:00Z\",11091,61032,\"ATLANTIC SOUTH\",\"Waterspout\",0,0,0,0,0,0,\"Trained Spotter\",\"MELBOURNE BEACH\",\"MELBOURNE BEACH\",28.0393,-80.6048,28.0393,-80.6048,\"Showers and thunderstorms lingering along the coast produced waterspouts in Brevard County.\",\"A waterspout formed in the Atlantic southeast of Melbourne Beach and briefly moved toward shore.\",{\"TotalDamages\":0,\"StartTime\":\"2007-09-29T08:11:00.0000000Z\",\"EndTime\":\"2007-09-29T08:11:00.0000000Z\",\"Details\":{\"Description\":\"A waterspout formed in the Atlantic southeast of Melbourne Beach and briefly moved toward shore.\",\"Location\":\"ATLANTIC SOUTH\"}}]]}\n,{\"FrameType\":\"TableProgress\",\"TableId\":1,\"TableProgress\":0.0}\n,{\"FrameType\":\"TableCompletion\",\"TableId\":1,\"RowCount\":1}\n,{\"FrameType\":\"DataSetCompletion\",\"HasErrors\":false,\"Cancelled\":false}\n]";

    #[allow(dead_code, non_snake_case)]
    #[derive(Debug, Deserialize)]
    struct StormEventsRow {
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

    impl KustoRow for StormEventsRow {
        fn validate(columns: &[(&str, &str)]) -> Result<(), Error> {
            let expected = [
                ("StartTime", "datetime"),
                ("EndTime", "datetime"),
                ("EpisodeId", "int"),
                ("EventId", "int"),
                ("State", "string"),
                ("EventType", "string"),
                ("InjuriesDirect", "int"),
                ("InjuriesIndirect", "int"),
                ("DeathsDirect", "int"),
                ("DeathsIndirect", "int"),
                ("DamageProperty", "int"),
                ("DamageCrops", "int"),
                ("Source", "string"),
                ("BeginLocation", "string"),
                ("EndLocation", "string"),
                ("BeginLat", "real"),
                ("BeginLon", "real"),
                ("EndLat", "real"),
                ("EndLon", "real"),
                ("EpisodeNarrative", "string"),
                ("EventNarrative", "string"),
                ("StormSummary", "dynamic"),
            ];
            if columns.len() != expected.len() {
                return Err(Error::RowType {
                    reason: format!("expected {} columns, got {}", expected.len(), columns.len()),
                });
            }
            for (got, want) in columns.iter().zip(expected.iter()) {
                if got != want {
                    return Err(Error::RowType {
                        reason: format!("expected {want:?}, got {got:?}"),
                    });
                }
            }
            Ok(())
        }
    }

    #[test]
    fn test_real_sequence() {
        let (result, rows) =
            run_response_parser_loop::<StormEventsRow>(&[STORMEVENTS_RAW_RESPONSE]);

        assert!(
            result.expect("real response should parse without error"),
            "real response should reach Complete",
        );
        assert_eq!(rows.len(), 1, "expected a single StormEvents row");

        let row = &rows[0];
        assert_eq!(row.State, "ATLANTIC SOUTH");
        assert_eq!(row.EventType, "Waterspout");
        assert_eq!(row.EpisodeId, 11091);
        assert_eq!(row.EventId, 61032);
        assert_eq!(row.StartTime.to_rfc3339(), "2007-09-29T08:11:00+00:00");
        assert_eq!(row.BeginLat, Some(28.0393));
        assert!(
            row.StormSummary.is_object(),
            "dynamic column should deserialise to a JSON object",
        );
    }

    #[test]
    fn test_real_sequence_survives_chunk_boundaries() {
        // The real response, split at every byte boundary, must parse identically.
        for split in 0..=STORMEVENTS_RAW_RESPONSE.len() {
            let (head, tail) = STORMEVENTS_RAW_RESPONSE.split_at(split);
            let (result, rows) = run_response_parser_loop::<StormEventsRow>(&[head, tail]);

            let complete = result.unwrap_or_else(|e| panic!("split {split}: {e}"));
            assert!(complete, "split {split}: did not reach Complete");
            assert_eq!(rows.len(), 1, "split {split}: wrong row count");
        }
    }
}
