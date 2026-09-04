//! Proves the reader_v2 surface the CXX bridge imports is reachable from
//! outside hudi-core. A compile failure here IS the test failure.

#[test]
fn reader_v2_surface_is_publicly_reachable() {
    // Type-level only: naming these paths from an integration test (a separate
    // crate) fails to compile unless the modules are `pub`.
    #[allow(unused_imports)]
    use hudi_core::file_group::reader_v2::{
        engine::HoodieFileGroupReader, input_split::InputSplit,
        merge_iterator::FileGroupMergeStream, reader_context::CompletionGateInputs,
        reader_context::ReaderContext, reader_parameters::ReaderParameters,
        record_context::RecordContext, schema_handler::FileGroupReaderSchemaHandler,
    };
    #[allow(unused_imports)]
    use hudi_core::timeline::selector::InstantRange;
}
