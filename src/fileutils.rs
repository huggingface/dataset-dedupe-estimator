use arrow_array::RecordBatchReader;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::fs::File;

fn parse_compression(s: &str) -> PyResult<Compression> {
    match s.to_lowercase().as_str() {
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => Ok(Compression::GZIP(GzipLevel::default())),
        "lz4" => Ok(Compression::LZ4),
        "lz4_raw" => Ok(Compression::LZ4_RAW),
        "zstd" => Ok(Compression::ZSTD(ZstdLevel::default())),
        "uncompressed" | "none" => Ok(Compression::UNCOMPRESSED),
        other => Err(PyValueError::new_err(format!(
            "Unknown compression: {other}"
        ))),
    }
}

fn build_writer_properties(cdc: bool, compression: Option<String>) -> PyResult<WriterProperties> {
    let mut builder = WriterProperties::builder();
    if cdc {
        builder = builder.set_content_defined_chunking(true);
    }
    if let Some(c) = compression {
        builder = builder.set_compression(parse_compression(&c)?);
    }
    Ok(builder.build())
}

pub(crate) fn rewrite_to_parquet_rs(
    src_path: String,
    dest_path: String,
    batch_size: usize,
    cdc: bool,
    compression: Option<String>,
) -> PyResult<()> {
    let input = File::open(&src_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to open {src_path}: {e}")))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(input)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create reader: {e}")))?
        .with_batch_size(batch_size)
        .build()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to build reader: {e}")))?;
    let schema = reader.schema().clone();

    let output = File::create(&dest_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create {dest_path}: {e}")))?;
    let props = build_writer_properties(cdc, compression)?;
    let mut writer = ArrowWriter::try_new(output, schema, Some(props))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create writer: {e}")))?;

    for maybe_batch in reader {
        let batch = maybe_batch
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to read batch: {e}")))?;
        writer
            .write(&batch)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to write batch: {e}")))?;
    }

    writer
        .close()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to close writer: {e}")))?;

    Ok(())
}
