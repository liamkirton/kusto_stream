use crate::Error;

pub trait KustoRow {
    fn validate(columns: &[(&str, &str)]) -> Result<(), Error>;
}
