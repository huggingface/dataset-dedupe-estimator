use pyo3::prelude::*;

mod show;
mod store;

use show::write_ppm;
use store::ChunkStore;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn estimate(file_paths: Vec<String>) -> PyResult<()> {
    let mut stores = ChunkStore::from_files(&file_paths)?;
    let merged = ChunkStore::merge(&mut stores);
    let (total, total_size, total_compressed) = merged.stats();
    println!(
        "Total: {}, Total Size: {}, Total Compressed: {}",
        total, total_size, total_compressed
    );

    for (store, file_path) in stores.iter().zip(file_paths.iter()) {
        let segments = store.segments();
        let output_file_path = format!("{}.ppm", file_path);
        write_ppm(&segments, &output_file_path)?;
    }

    // let output_file_path = format!("{}.ppm", file_paths.last().unwrap());
    // write_ppm(&merged.segments(), &output_file_path)?;

    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(estimate, m)?)?;
    Ok(())
}
