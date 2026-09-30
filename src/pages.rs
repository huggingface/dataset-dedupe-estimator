//! Exact page layout of a parquet file, decoded from the page headers.
//!
//! Walks every column chunk the same way arrow-rs' `parquet-layout` binary does,
//! using the deprecated thrift `PageHeader` (removed in parquet 59).
#![allow(deprecated)]

use std::fs::File;

use parquet::errors::Result;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::reader::ChunkReader;
use parquet::format::PageHeader;
use parquet::thrift::TSerializable;
use pyo3::IntoPyObject;
use thrift::protocol::TCompactInputProtocol;
use xxhash_rust::xxh3::xxh3_64;

/// One page of a column chunk as it is laid out in the file.
#[derive(IntoPyObject)]
pub struct Page {
    pub row_group: usize,
    /// Leaf column index in the parquet schema.
    pub column: usize,
    pub path: String,
    /// "dictionary", "data_page_v1" or "data_page_v2".
    pub page_type: &'static str,
    pub encoding: &'static str,
    /// File offset of the page header.
    pub offset: u64,
    pub header_bytes: usize,
    pub compressed_bytes: usize,
    pub uncompressed_bytes: usize,
    /// Number of levels (values including nulls) for data pages, entries for dictionary pages.
    pub num_values: usize,
    /// Hash of the (compressed) page payload.
    pub payload_hash: u64,
    /// Hash of the header and payload, the bytes a content addressed store sees.
    pub page_hash: u64,
}

fn encoding(encoding: i32) -> &'static str {
    match encoding {
        0 => "plain",
        2 => "plain_dictionary",
        3 => "rle",
        4 => "bit_packed",
        5 => "delta_binary_packed",
        6 => "delta_length_byte_array",
        7 => "delta_byte_array",
        8 => "rle_dictionary",
        9 => "byte_stream_split",
        _ => "unknown",
    }
}

/// Read all dictionary and data pages of a parquet file in file order.
pub fn read_pages(path: &str) -> Result<Vec<Page>> {
    let file = File::open(path)?;
    let metadata = ParquetMetaDataReader::new().parse_and_finish(&file)?;
    let schema = metadata.file_metadata().schema_descr();

    let mut pages = Vec::new();
    for (row_group, row_group_meta) in metadata.row_groups().iter().enumerate() {
        for (column, (chunk, descr)) in row_group_meta
            .columns()
            .iter()
            .zip(schema.columns())
            .enumerate()
        {
            let (start, length) = chunk.byte_range();
            let bytes = file.get_bytes(start, length as usize)?;
            let mut offset = 0;
            while offset < bytes.len() {
                let mut remaining = &bytes[offset..];
                let header = {
                    let mut protocol = TCompactInputProtocol::new(&mut remaining);
                    PageHeader::read_from_in_protocol(&mut protocol)?
                };
                let header_bytes = bytes.len() - offset - remaining.len();
                let end = offset + header_bytes + header.compressed_page_size as usize;

                let (page_type, page_encoding, num_values) =
                    if let Some(h) = &header.dictionary_page_header {
                        ("dictionary", h.encoding.0, h.num_values)
                    } else if let Some(h) = &header.data_page_header {
                        ("data_page_v1", h.encoding.0, h.num_values)
                    } else if let Some(h) = &header.data_page_header_v2 {
                        ("data_page_v2", h.encoding.0, h.num_values)
                    } else {
                        // index pages carry no values
                        offset = end;
                        continue;
                    };

                pages.push(Page {
                    row_group,
                    column,
                    path: descr.path().string(),
                    page_type,
                    encoding: encoding(page_encoding),
                    offset: start + offset as u64,
                    header_bytes,
                    compressed_bytes: header.compressed_page_size as usize,
                    uncompressed_bytes: header.uncompressed_page_size as usize,
                    num_values: num_values as usize,
                    payload_hash: xxh3_64(&bytes[offset + header_bytes..end]),
                    page_hash: xxh3_64(&bytes[offset..end]),
                });
                offset = end;
            }
        }
    }
    Ok(pages)
}
