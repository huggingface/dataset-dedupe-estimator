use anyhow::Result;
use deduplication::constants::TARGET_CHUNK_SIZE;
use deduplication::Chunker;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read};

const READ_BUFFER_SIZE: usize = 4 * 1024 * 1024;

pub fn dedup_estimate(file_paths: Vec<String>) -> Result<u64> {
    let mut seen = HashSet::new();
    let mut unique_bytes: u64 = 0;

    for path in &file_paths {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut chunker = Chunker::new(*TARGET_CHUNK_SIZE);
        let mut buf = vec![0u8; READ_BUFFER_SIZE];

        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                if let Some(chunk) = chunker.finish() {
                    if seen.insert(chunk.hash) {
                        unique_bytes += chunk.data.len() as u64;
                    }
                }
                break;
            }
            for chunk in chunker.next_block(&buf[..n], false) {
                if seen.insert(chunk.hash) {
                    unique_bytes += chunk.data.len() as u64;
                }
            }
        }
    }

    Ok(unique_bytes)
}
