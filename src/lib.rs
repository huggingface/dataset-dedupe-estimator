use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::path::Path;

mod show;
mod store;

use show::write_png;
use store::{Chunk, ChunkStore};

/// Formats the sum of two numbers as string.
#[pyfunction]
fn estimate(file_paths: Vec<String>) -> PyResult<(usize, usize, usize)> {
    let mut stores = ChunkStore::from_files(&file_paths, false)?;
    let merged = ChunkStore::merge(&mut stores, false);

    for (store, file_path) in stores.iter().zip(file_paths.iter()) {
        let segments = store.segments();
        let output_file_path = format!("{}.png", file_path);
        write_png(&segments, &output_file_path)?;
    }

    let file_dir = Path::new(file_paths.last().unwrap()).parent().unwrap();
    let output_file_path = file_dir.join("merged.png");
    write_png(&merged.segments(), &output_file_path.to_str().unwrap())?;

    Ok(merged.stats())
}

#[pyfunction]
fn chunks(data: Vec<String>) -> PyResult<HashMap<u64, Chunk>> {
    let mut stores = ChunkStore::from_strings(&data, true)?;
    let merged = ChunkStore::merge(&mut stores, true);
    Ok(merged.chunks())
}

/// A Python module implemented in Rust.
#[pymodule]
fn core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(estimate, m)?)?;
    m.add_function(wrap_pyfunction!(chunks, m)?)?;
    Ok(())
}
