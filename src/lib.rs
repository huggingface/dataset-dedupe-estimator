use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::path::Path;

mod fileutils;
mod pages;
mod show;
mod store;
mod xet;

use fileutils::{
    rewrite_to_parquet_rs as _rewrite_to_parquet_rs, write_to_parquet_rs as _write_to_parquet_rs,
    WriterOptions,
};
use pages::{read_pages, Page};
use show::write_png;
use store::{Chunk, ChunkStore};

/// Estimate the deduplication of the files with content defined chunks.
///
/// With `plot`, also writes the chunks of each file next to it as `<file>.png`, and
/// of all files as `merged.png` next to the last one.
#[pyfunction]
#[pyo3(signature = (file_paths, plot = true))]
fn estimate(
    py: Python<'_>,
    file_paths: Vec<String>,
    plot: bool,
) -> PyResult<(usize, usize, usize)> {
    py.detach(|| {
        let mut stores = ChunkStore::from_files(&file_paths, false)?;
        let merged = ChunkStore::merge(&mut stores, false);

        if plot {
            for (store, file_path) in stores.iter().zip(file_paths.iter()) {
                let segments = store.segments();
                let output_file_path = format!("{}.png", file_path);
                write_png(&segments, &output_file_path)?;
            }

            let file_dir = Path::new(file_paths.last().unwrap()).parent().unwrap();
            let output_file_path = file_dir.join("merged.png");
            write_png(&merged.segments(), output_file_path.to_str().unwrap())?;
        }

        Ok(merged.stats())
    })
}

#[pyfunction]
#[pyo3(signature = (file_paths, store_data = false))]
fn chunks(
    py: Python<'_>,
    file_paths: Vec<String>,
    store_data: bool,
) -> PyResult<Vec<(u64, Chunk)>> {
    py.detach(|| {
        let mut stores = ChunkStore::from_files(&file_paths, store_data)?;
        let merged = ChunkStore::merge(&mut stores, store_data);
        Ok(merged.chunks())
    })
}

/// Rewrite a parquet file with arrow-rs using the options of a `de.formats.Parquet` instance.
#[pyfunction]
fn rewrite_to_parquet_rs(
    py: Python<'_>,
    src_path: String,
    dest_path: String,
    options: WriterOptions,
) -> PyResult<()> {
    py.detach(|| _rewrite_to_parquet_rs(&src_path, &dest_path, &options))
}

/// Write the record batches of an object with `__arrow_c_stream__`, e.g. a
/// `pa.RecordBatchReader`, with arrow-rs using the options of a `de.formats.Parquet`
/// instance, one write call per batch.
#[pyfunction]
fn write_to_parquet_rs(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    dest_path: String,
    options: WriterOptions,
) -> PyResult<()> {
    let capsule = data
        .call_method0("__arrow_c_stream__")?
        .cast_into::<PyCapsule>()?;
    let stream = capsule.pointer_checked(Some(c"arrow_array_stream"))?;
    // takes the stream over, leaving a released one to the capsule's destructor
    let reader =
        unsafe { ArrowArrayStreamReader::from_raw(stream.cast::<FFI_ArrowArrayStream>().as_ptr()) }
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to import the stream: {e}")))?;
    py.detach(|| _write_to_parquet_rs(reader, &dest_path, &options))
}

/// Pages of a parquet file in file order, decoded from the page headers.
#[pyfunction]
fn parquet_pages(py: Python<'_>, path: String) -> PyResult<Vec<Page>> {
    py.detach(|| read_pages(&path).map_err(|e| PyRuntimeError::new_err(e.to_string())))
}

#[pyfunction]
fn estimate_xet(py: Python<'_>, file_paths: Vec<String>) -> PyResult<u64> {
    py.detach(|| {
        xet::dedup_estimate(file_paths).map_err(|e| PyRuntimeError::new_err(e.to_string()))
    })
}

/// A Python module implemented in Rust.
#[pymodule]
fn core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(estimate, m)?)?;
    m.add_function(wrap_pyfunction!(chunks, m)?)?;
    m.add_function(wrap_pyfunction!(rewrite_to_parquet_rs, m)?)?;
    m.add_function(wrap_pyfunction!(write_to_parquet_rs, m)?)?;
    m.add_function(wrap_pyfunction!(parquet_pages, m)?)?;
    m.add_function(wrap_pyfunction!(estimate_xet, m)?)?;
    Ok(())
}
