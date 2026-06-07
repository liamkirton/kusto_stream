use serde_json::Value;

use crate::{KustoDateTime, KustoGuid};

pub trait KustoScalar {
    fn accepts(kusto_type: &str) -> bool;
}

impl KustoScalar for bool {
    fn accepts(k: &str) -> bool {
        k == "bool"
    }
}

impl KustoScalar for i32 {
    fn accepts(k: &str) -> bool {
        k == "int"
    }
}

impl KustoScalar for i64 {
    fn accepts(k: &str) -> bool {
        k == "long"
    }
}

impl KustoScalar for f64 {
    fn accepts(k: &str) -> bool {
        k == "real" || k == "decimal"
    }
}

impl KustoScalar for String {
    fn accepts(k: &str) -> bool {
        k == "string" || k == "timespan" // TODO: proper timespan support
    }
}

impl KustoScalar for KustoDateTime {
    fn accepts(k: &str) -> bool {
        k == "datetime"
    }
}

impl KustoScalar for KustoGuid {
    fn accepts(k: &str) -> bool {
        k == "guid"
    }
}

impl KustoScalar for Value {
    fn accepts(_k: &str) -> bool {
        true
    }
}

impl<T: KustoScalar> KustoScalar for Option<T> {
    fn accepts(k: &str) -> bool {
        T::accepts(k)
    }
}
