// Rewrite a parquet file with parquet-cpp in write calls of batch_size rows, used by
// de.formats.ParquetCpp, built against pyarrow's own Arrow and Parquet libraries.
//
// usage: ParquetWrite SRC DEST batch_size=N [KEY=VALUE...]
//
// pyarrow's ParquetWriter ends a row group at every write call, while
// FileWriter::WriteRecordBatch writes into buffered row groups, which end at
// row_group_size rows only, so the write calls don't cut the row groups.
//
// The options mirror de.formats.Parquet: use_cdc=false|true|MIN,MAX,NORM,
// compression, use_dictionary, row_group_size (rows), data_page_size (bytes),
// max_rows_per_page, data_page_version=1.0|2.0 and column_encoding=PATH:ENCODING,...
// Options left out keep the defaults of pq.write_table, and the page statistics go to
// the page index as ParquetCpp writes them.

#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/util/compression.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

namespace {

void Check(const arrow::Status& status) {
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
}

template <typename T>
T Unwrap(arrow::Result<T> result) {
  Check(result.status());
  return std::move(result).ValueUnsafe();
}

std::vector<std::string> Split(const std::string& value, char separator) {
  std::vector<std::string> parts;
  std::stringstream stream(value);
  for (std::string part; std::getline(stream, part, separator);) {
    parts.push_back(part);
  }
  return parts;
}

parquet::Encoding::type ParseEncoding(const std::string& name) {
  if (name == "DELTA_BINARY_PACKED") return parquet::Encoding::DELTA_BINARY_PACKED;
  if (name == "DELTA_BYTE_ARRAY") return parquet::Encoding::DELTA_BYTE_ARRAY;
  if (name == "PLAIN") return parquet::Encoding::PLAIN;
  throw std::invalid_argument("unknown encoding: " + name);
}

std::shared_ptr<parquet::WriterProperties> BuildProperties(
    const std::map<std::string, std::string>& options) {
  parquet::WriterProperties::Builder builder;
  builder.enable_write_page_index();
  for (const auto& [key, value] : options) {
    if (key == "use_cdc") {
      if (value == "true") {
        builder.enable_content_defined_chunking();
      } else if (value != "false") {
        auto sizes = Split(value, ',');
        builder.enable_content_defined_chunking();
        builder.content_defined_chunking_options(
            {std::stoll(sizes.at(0)), std::stoll(sizes.at(1)), std::stoi(sizes.at(2))});
      }
    } else if (key == "compression") {
      builder.compression(value == "none"
                              ? arrow::Compression::UNCOMPRESSED
                              : Unwrap(arrow::util::Codec::GetCompressionType(value)));
    } else if (key == "use_dictionary") {
      if (value == "true") {
        builder.enable_dictionary();
      } else {
        builder.disable_dictionary();
      }
    } else if (key == "row_group_size") {
      builder.max_row_group_length(std::stoll(value));
    } else if (key == "data_page_size") {
      builder.data_pagesize(std::stoll(value));
    } else if (key == "max_rows_per_page") {
      builder.max_rows_per_page(std::stoll(value));
    } else if (key == "data_page_version") {
      if (value != "1.0" && value != "2.0") {
        throw std::invalid_argument("unknown data page version: " + value);
      }
      builder.data_page_version(value == "2.0" ? parquet::ParquetDataPageVersion::V2
                                               : parquet::ParquetDataPageVersion::V1);
    } else if (key == "column_encoding") {
      for (const auto& column : Split(value, ',')) {
        auto separator = column.rfind(':');
        auto path = column.substr(0, separator);
        builder.disable_dictionary(path);
        builder.encoding(path, ParseEncoding(column.substr(separator + 1)));
      }
    } else if (key != "batch_size") {
      throw std::invalid_argument("unknown option: " + key);
    }
  }
  return builder.build();
}

void Rewrite(const std::string& src, const std::string& dest,
             const std::map<std::string, std::string>& options) {
  auto batch_size = std::stoll(options.at("batch_size"));

  auto input = Unwrap(arrow::io::ReadableFile::Open(src));
  auto reader =
      Unwrap(parquet::arrow::OpenFile(input, arrow::default_memory_pool()));
  std::shared_ptr<arrow::Table> table;
  Check(reader->ReadTable(&table));
  // a single chunk, so that every batch has batch_size rows
  table = Unwrap(table->CombineChunks());

  auto output = Unwrap(arrow::io::FileOutputStream::Open(dest));
  auto arrow_properties = parquet::ArrowWriterProperties::Builder().store_schema()->build();
  auto writer = Unwrap(parquet::arrow::FileWriter::Open(
      *table->schema(), arrow::default_memory_pool(), output, BuildProperties(options),
      arrow_properties));

  arrow::TableBatchReader batches(*table);
  batches.set_chunksize(batch_size);
  for (std::shared_ptr<arrow::RecordBatch> batch;;) {
    Check(batches.ReadNext(&batch));
    if (batch == nullptr) break;
    Check(writer->WriteRecordBatch(*batch));
  }
  Check(writer->Close());
  Check(output->Close());
}

}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc < 3) {
      throw std::invalid_argument("usage: ParquetWrite SRC DEST batch_size=N [KEY=VALUE...]");
    }
    std::map<std::string, std::string> options;
    for (int i = 3; i < argc; ++i) {
      std::string option = argv[i];
      auto separator = option.find('=');
      options[option.substr(0, separator)] = option.substr(separator + 1);
    }
    Rewrite(argv[1], argv[2], options);
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
  return 0;
}
