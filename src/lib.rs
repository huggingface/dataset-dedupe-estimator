use pyo3::prelude::*;
use std::path::Path;

mod show;
mod store;

use show::write_ppm;
use store::ChunkStore;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn estimate(file_paths: Vec<String>) -> PyResult<(usize, usize, usize)> {
    let mut stores = ChunkStore::from_files(&file_paths)?;
    for (store, file_path) in stores.iter().zip(file_paths.iter()) {
        let segments = store.segments();
        let output_file_path = format!("{}.ppm", file_path);
        write_ppm(&segments, &output_file_path)?;
    }

    let merged = ChunkStore::merge(&mut stores);
    let file_dir = Path::new(file_paths.last().unwrap()).parent().unwrap();
    let output_file_path = file_dir.join("merged.ppm");
    write_ppm(&merged.segments(), &output_file_path.to_str().unwrap())?;

    Ok(merged.stats())
}

/// A Python module implemented in Rust.
#[pymodule]
fn core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(estimate, m)?)?;
    Ok(())
}
