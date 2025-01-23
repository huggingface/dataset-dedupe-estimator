use gearhash::Hasher;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use xxhash_rust::xxh3::xxh3_64;

const MASK: u64 = 0xffff00000000000;

// TODO(kszucs): add min_size and max_size for the chunking

#[derive(Debug, Clone)]
pub(crate) struct Chunk {
    size: usize,
    compressed: usize,
    first_seen_in: i64,
}

#[derive(Debug, Default)]
pub(crate) struct ChunkStore {
    total: usize,
    order: Vec<u64>,
    chunks: HashMap<u64, Chunk>,
}

impl ChunkStore {
    pub fn from_stream<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut store = ChunkStore::default();
        let mut hasher = Hasher::default();
        let mut buffer = [0; 1024];

        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            store.total += bytes_read;

            let mut start = 0;
            while let Some(size) = hasher.next_match(&buffer[start..bytes_read], MASK) {
                let hash = xxh3_64(&buffer[start..start + size]);
                store.order.push(hash);
                if !store.chunks.contains_key(&hash) {
                    let chunk = Chunk {
                        size: size,
                        compressed: 0, // Placeholder for compressed size
                        first_seen_in: 0,
                    };
                    store.chunks.insert(hash, chunk);
                }
                start += size;
            }
        }
        Ok(store)
    }

    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        Self::from_stream(&mut reader)
    }

    pub fn from_files<P: AsRef<Path> + Send + Sync>(
        paths: &[P],
    ) -> Result<Vec<Self>, std::io::Error> {
        return paths
            .par_iter()
            .map(|path| ChunkStore::from_file(path))
            .collect();
    }

    pub fn merge(stores: &mut [ChunkStore]) -> Self {
        let mut merged = ChunkStore::default();

        for (index, store) in stores.iter_mut().enumerate() {
            merged.total += store.total;
            merged.order.extend(store.order.iter());
            for (hash, chunk) in &mut store.chunks {
                merged.chunks.entry(*hash).or_insert_with(|| {
                    chunk.first_seen_in = index as i64;
                    chunk.clone()
                });
            }
        }

        merged
    }

    pub fn stats(&self) -> (usize, usize, usize) {
        let total_size = self.chunks.values().map(|chunk| chunk.size).sum();
        let total_compressed = self.chunks.values().map(|chunk| chunk.compressed).sum();
        (self.total, total_size, total_compressed)
    }

    pub fn segments(&self) -> Vec<usize> {
        self.order.iter().map(|hash| self.chunks[hash].first_seen_in as usize).collect()
    }
}
