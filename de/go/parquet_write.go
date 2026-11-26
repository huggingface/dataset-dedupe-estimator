// Write parquet files with arrow-go, used by de.formats.ParquetGo.
//
// usage: parquet-go DEST KEY=VALUE... < ARROW_IPC_STREAM
//
// Every record batch of the Arrow IPC stream on stdin is a write call, the row
// groups are cut by row_group_size across them. Errors are printed to stderr with
// exit status 1.
//
// The options mirror de.formats.Parquet: use_cdc=false|true|MIN,MAX,NORM,
// compression, use_dictionary, row_group_size (rows), data_page_size (bytes) and
// data_page_version=1.0|2.0. arrow-go has no row limit per page, max_rows_per_page
// only takes de's NO_LIMIT. Options left out keep the arrow-go defaults.
//
// Build it against an arrow-go checkout, from the checkout's directory:
//
//	go build -o parquet-go /path/to/de/de/go/parquet_write.go
package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// de's NO_LIMIT, a page row limit large enough to never cut
const noLimit = 1 << 30

// the encodings of parquet-java's and parquet-rs's writer version 2.0 for values
// that aren't dictionary encoded, by physical type, as in de.formats.V2_ENCODINGS
var v2Encodings = map[parquet.Type]parquet.Encoding{
	parquet.Types.Int32:             parquet.Encodings.DeltaBinaryPacked,
	parquet.Types.Int64:             parquet.Encodings.DeltaBinaryPacked,
	parquet.Types.ByteArray:         parquet.Encodings.DeltaByteArray,
	parquet.Types.FixedLenByteArray: parquet.Encodings.DeltaByteArray,
}

func main() {
	if err := write(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func write(dest string, options []string) error {
	reader, err := ipc.NewReader(bufio.NewReader(os.Stdin))
	if err != nil {
		return err
	}
	defer reader.Release()

	settings, err := writerProperties(options)
	if err != nil {
		return err
	}
	props := parquet.NewWriterProperties(settings...)
	// the arrow schema is stored, so arrow-only types read back as they were written
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	if props.DataPageVersion() == parquet.DataPageV2 && !props.DictionaryEnabled() {
		// encodings are set per leaf column, of the schema the writer derives
		schema, err := pqarrow.ToParquet(reader.Schema(), props, arrowProps)
		if err != nil {
			return err
		}
		for i := 0; i < schema.NumColumns(); i++ {
			column := schema.Column(i)
			if encoding, ok := v2Encodings[column.PhysicalType()]; ok {
				settings = append(settings, parquet.WithEncodingFor(column.Path(), encoding))
			}
		}
		props = parquet.NewWriterProperties(settings...)
	}

	file, err := os.Create(dest)
	if err != nil {
		return err
	}
	writer, err := pqarrow.NewFileWriter(reader.Schema(), file, props, arrowProps)
	if err != nil {
		file.Close()
		return err
	}
	for reader.Next() {
		if err := writer.WriteBuffered(reader.RecordBatch()); err != nil {
			writer.Close()
			return err
		}
	}
	if err := reader.Err(); err != nil {
		writer.Close()
		return err
	}
	return writer.Close()
}

// writerProperties parses the KEY=VALUE options.
func writerProperties(options []string) ([]parquet.WriterProperty, error) {
	var props []parquet.WriterProperty
	for _, option := range options {
		key, value, _ := strings.Cut(option, "=")
		switch key {
		case "use_cdc":
			if min, rest, ok := strings.Cut(value, ","); ok {
				max, norm, _ := strings.Cut(rest, ",")
				cdc := parquet.CDCOptions{}
				var err error
				if cdc.MinChunkSize, err = strconv.ParseInt(min, 10, 64); err != nil {
					return nil, err
				}
				if cdc.MaxChunkSize, err = strconv.ParseInt(max, 10, 64); err != nil {
					return nil, err
				}
				if cdc.NormLevel, err = strconv.Atoi(norm); err != nil {
					return nil, err
				}
				props = append(props, parquet.WithContentDefinedChunking(cdc))
			} else if enabled, err := strconv.ParseBool(value); err != nil {
				return nil, err
			} else if enabled {
				props = append(props, parquet.WithContentDefinedChunking(parquet.DefaultCDCOptions()))
			}
		case "compression":
			if strings.EqualFold(value, "none") {
				value = "uncompressed"
			}
			var codec compress.Compression
			if err := codec.UnmarshalText([]byte(strings.ToUpper(value))); err != nil {
				return nil, fmt.Errorf("compression %s: %w", value, err)
			}
			props = append(props, parquet.WithCompression(codec))
		case "use_dictionary":
			enabled, err := strconv.ParseBool(value)
			if err != nil {
				return nil, err
			}
			props = append(props, parquet.WithDictionaryDefault(enabled))
		case "row_group_size":
			rows, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, err
			}
			props = append(props, parquet.WithMaxRowGroupLength(rows))
		case "data_page_size":
			size, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, err
			}
			props = append(props, parquet.WithDataPageSize(size))
		case "max_rows_per_page":
			if value != strconv.Itoa(noLimit) {
				return nil, fmt.Errorf("arrow-go has no row limit per page: %s", value)
			}
		case "data_page_version":
			switch value {
			case "1.0":
				props = append(props, parquet.WithDataPageVersion(parquet.DataPageV1))
			case "2.0":
				props = append(props, parquet.WithDataPageVersion(parquet.DataPageV2))
			default:
				return nil, fmt.Errorf("unknown data page version: %s", value)
			}
		default:
			return nil, fmt.Errorf("unknown option: %s", key)
		}
	}
	return props, nil
}
