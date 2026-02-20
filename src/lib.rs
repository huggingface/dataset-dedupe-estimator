use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::path::Path;

mod fileutils;
mod show;
mod store;
mod xet;

use fileutils::rewrite_to_parquet_rs as _rewrite_to_parquet_rs;
use show::write_png;
use store::{Chunk, ChunkStore};

/// Formats the sum of two numbers as string.
#[pyfunction]
fn estimate(py: Python<'_>, file_paths: Vec<String>) -> PyResult<(usize, usize, usize)> {
    py.allow_threads(|| {
        let mut stores = ChunkStore::from_files(&file_paths, false)?;
        let merged = ChunkStore::merge(&mut stores, false);

        for (store, file_path) in stores.iter().zip(file_paths.iter()) {
            let segments = store.segments();
            let output_file_path = format!("{}.png", file_path);
            write_png(&segments, &output_file_path)?;
        }

        let file_dir = Path::new(file_paths.last().unwrap()).parent().unwrap();
        let output_file_path = file_dir.join("merged.png");
        write_png(&merged.segments(), output_file_path.to_str().unwrap())?;

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
    py.allow_threads(|| {
        let mut stores = ChunkStore::from_files(&file_paths, store_data)?;
        let merged = ChunkStore::merge(&mut stores, store_data);
        Ok(merged.chunks())
    })
}

#[pyfunction]
#[pyo3(signature = (src_path, dest_path, batch_size = 1024 * 1024, cdc = false, compression = None))]
fn rewrite_to_parquet_rs(
    py: Python<'_>,
    src_path: String,
    dest_path: String,
    batch_size: usize,
    cdc: bool,
    compression: Option<String>,
) -> PyResult<()> {
    py.allow_threads(|| _rewrite_to_parquet_rs(src_path, dest_path, batch_size, cdc, compression))
}

#[pyfunction]
fn estimate_xet(py: Python<'_>, file_paths: Vec<String>) -> PyResult<u64> {
    py.allow_threads(|| {
        xet::dedup_estimate(file_paths).map_err(|e| PyRuntimeError::new_err(e.to_string()))
    })
}

/// A Python module implemented in Rust.
#[pymodule]
fn core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(estimate, m)?)?;
    m.add_function(wrap_pyfunction!(chunks, m)?)?;
    m.add_function(wrap_pyfunction!(rewrite_to_parquet_rs, m)?)?;
    m.add_function(wrap_pyfunction!(estimate_xet, m)?)?;
    Ok(())
}
