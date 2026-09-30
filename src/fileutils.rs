use arrow_array::RecordBatchReader;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::{CdcOptions, WriterProperties};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::fs::File;

/// Mirrors `use_cdc: bool | CdcParams` of the Python `Parquet` format.
#[derive(FromPyObject)]
pub(crate) enum Cdc {
    Flag(bool),
    Params {
        min_chunk_size: usize,
        max_chunk_size: usize,
        norm_level: i32,
    },
}

/// Writer parameters read by attribute from a Python `Parquet` format instance.
#[derive(FromPyObject)]
pub(crate) struct WriterOptions {
    use_cdc: Cdc,
    compression: Option<String>,
    use_dictionary: bool,
    row_group_size: Option<usize>,
    data_page_size: Option<usize>,
    max_rows_per_page: Option<usize>,
    /// `None` writes all rows with a single `ArrowWriter::write` call.
    batch_size: Option<usize>,
}

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

fn build_writer_properties(options: &WriterOptions) -> PyResult<WriterProperties> {
    let cdc = match options.use_cdc {
        Cdc::Flag(false) => None,
        Cdc::Flag(true) => Some(CdcOptions::default()),
        Cdc::Params {
            min_chunk_size,
            max_chunk_size,
            norm_level,
        } => Some(CdcOptions {
            min_chunk_size,
            max_chunk_size,
            norm_level,
        }),
    };
    let mut builder = WriterProperties::builder()
        .set_dictionary_enabled(options.use_dictionary)
        .set_content_defined_chunking(cdc);
    if let Some(c) = &options.compression {
        builder = builder.set_compression(parse_compression(c)?);
    }
    if let Some(n) = options.row_group_size {
        builder = builder.set_max_row_group_row_count(Some(n));
    }
    if let Some(n) = options.data_page_size {
        builder = builder.set_data_page_size_limit(n);
    }
    if let Some(n) = options.max_rows_per_page {
        builder = builder.set_data_page_row_count_limit(n);
    }
    Ok(builder.build())
}

pub(crate) fn rewrite_to_parquet_rs(
    src_path: &str,
    dest_path: &str,
    options: &WriterOptions,
) -> PyResult<()> {
    let input = File::open(src_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to open {src_path}: {e}")))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(input)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create reader: {e}")))?
        .with_batch_size(options.batch_size.unwrap_or(usize::MAX))
        .build()
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to build reader: {e}")))?;
    let schema = reader.schema().clone();

    let output = File::create(dest_path)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to create {dest_path}: {e}")))?;
    let props = build_writer_properties(options)?;
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
