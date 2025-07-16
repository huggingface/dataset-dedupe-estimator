use gearhash::Hasher;
use indicatif::{ParallelProgressIterator, ProgressIterator};
use lz4_flex::block;
use pyo3::IntoPyObject;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use xxhash_rust::xxh3::xxh3_64;

const MASK: u64 = 0xffff000000000000;
const MIN_LEN: usize = 65536 / 8;
const MAX_LEN: usize = 65536 * 2;
const READ_BUFFER_SIZE: usize = 1024 * 1024;

#[derive(Debug, Clone, IntoPyObject)]
pub(crate) struct Chunk {
    size: usize,
    compressed: usize,
    seen_in: Vec<i64>,
    first_seen_in: i64,
    data: Option<Vec<u8>>,
}

#[derive(Debug, Default)]
pub(crate) struct ChunkStore {
    total: usize,
    order: Vec<u64>,
    chunks: HashMap<u64, Chunk>,
    store_data: bool,
}

impl ChunkStore {
    pub fn new(store_data: bool) -> Self {
        ChunkStore {
            total: 0,
            order: Vec::new(),
            chunks: HashMap::new(),
            store_data,
        }
    }

    pub fn add(&mut self, chunk: &[u8]) {
        let hash = xxh3_64(chunk);
        let comp = block::compress(chunk);
        self.total += chunk.len();
        self.order.push(hash);

        let data = if self.store_data {
            Some(chunk.to_vec())
        } else {
            None
        };

        let chunk = Chunk {
            size: chunk.len(),
            compressed: comp.len(),
            seen_in: vec![],
            first_seen_in: 0,
            data,
        };
        self.chunks.insert(hash, chunk);
    }

    pub fn from_stream<R: Read>(reader: &mut R, store_data: bool) -> Result<Self, std::io::Error> {
        let mut store = ChunkStore::new(store_data);
        let mut hasher = Hasher::default();
        let mut buffer = [0; READ_BUFFER_SIZE];
        let mut chunk = Vec::<u8>::with_capacity(MAX_LEN);

        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            let mut start = 0;
            while let Some(size) = hasher.next_match(&buffer[start..bytes_read], MASK) {
                chunk.extend_from_slice(&buffer[start..start + size]);
                start += size;

                // TODO(kszucs): MAX_LEN is not implemented yet
                if chunk.len() >= MIN_LEN {
                    store.add(&chunk);
                    chunk.clear();
                }
            }
            chunk.extend_from_slice(&buffer[start..bytes_read]);
        }

        // add remaining as last chunk
        store.add(&chunk);

        Ok(store)
    }

    pub fn from_strings(data: &[String], store_data: bool) -> Result<Vec<Self>, std::io::Error> {
        data.iter()
            .progress_count(data.len() as u64)
            .map(|bytes| ChunkStore::from_stream(&mut bytes.as_bytes(), store_data))
            .collect()
    }

    pub fn from_file<P: AsRef<Path>>(path: P, store_data: bool) -> Result<Self, std::io::Error> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        Self::from_stream(&mut reader, store_data)
    }

    pub fn from_files<P: AsRef<Path> + Send + Sync>(
        paths: &[P],
        store_data: bool,
    ) -> Result<Vec<Self>, std::io::Error> {
        paths
            .par_iter()
            .progress_count(paths.len() as u64)
            .map(|path| ChunkStore::from_file(path, store_data))
            .collect()
    }

    pub fn merge(stores: &mut [ChunkStore], store_data: bool) -> Self {
        let mut merged = ChunkStore::new(store_data);

        for (index, store) in stores.iter_mut().enumerate() {
            merged.total += store.total;
            merged.order.extend(store.order.iter());
            for (hash, chunk) in &mut store.chunks {
                let entry = merged.chunks.entry(*hash).or_insert_with(|| {
                    chunk.first_seen_in = index as i64;
                    chunk.clone()
                });
                entry.seen_in.push(index as i64);
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
        self.order
            .iter()
            .map(|hash| self.chunks[hash].first_seen_in as usize)
            .collect()
    }

    pub fn chunks(&self) -> Vec<(u64, Chunk)> {
        self.order
            .iter()
            .map(|hash| (*hash, self.chunks[hash].clone()))
            .collect()
    }
}
