#![allow(dead_code)]

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(tag = "FrameType")]
pub(crate) enum KustoResponseV2<T> {
    DataSetHeader(DataSetHeader),
    DataSetCompletion(DataSetCompletion),
    DataTable(DataTable),
    TableHeader(TableHeader),
    TableFragment(TableFragment<T>),
    TableProgress(TableProgress),
    TableCompletion(TableCompletion),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct DataSetHeader {
    pub version: String,
    pub is_progressive: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct DataSetCompletion {
    pub has_errors: bool,
    pub cancelled: bool,
    pub one_api_errors: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct ColumnDef {
    pub column_name: String,
    pub column_type: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct DataTable {
    pub table_id: usize,
    pub table_kind: String,
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub rows: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct TableHeader {
    pub table_id: usize,
    pub table_kind: String,
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Deserialize)]
pub(crate) enum TableFragmentType {
    DataAppend,
    DataReplace,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct TableFragment<T> {
    pub table_id: usize,
    pub field_count: Option<usize>,
    pub table_fragment_type: TableFragmentType,
    pub rows: Vec<T>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct TableProgress {
    pub table_id: usize,
    pub table_progress: f32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct TableCompletion {
    pub table_id: usize,
    pub row_count: usize,
}
