#[derive(PartialEq)]
pub(crate) enum KustoResponseState {
    New,
    RequireResponseSchema,
    Streaming,
    Complete,
    CompleteWithErrors,
}
