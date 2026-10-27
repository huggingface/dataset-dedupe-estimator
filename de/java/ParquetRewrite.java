// Rewrite a parquet file with parquet-java, used by de.formats.ParquetJava.
//
// usage: java -cp <parquet-hadoop classpath> ParquetRewrite.java SRC DEST [KEY=VALUE...]
//
// The options mirror de.formats.Parquet: use_cdc=false|true|MIN,MAX,NORM,
// compression, use_dictionary, row_group_size (rows), data_page_size (bytes) and
// max_rows_per_page. Options left out keep the parquet-java defaults.

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

public class ParquetRewrite {
  public static void main(String[] args) throws Exception {
    Path src = new Path(args[0]);
    Path dest = new Path(args[1]);
    Map<String, String> options = new LinkedHashMap<>();
    for (int i = 2; i < args.length; i++) {
      String[] option = args[i].split("=", 2);
      options.put(option[0], option[1]);
    }

    // the raw local file system doesn't write .crc files next to the output
    Configuration conf = new Configuration();
    conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
    MessageType schema;
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(src, conf))) {
      schema = reader.getFooter().getFileMetaData().getSchema();
    }

    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(dest)
        .withConf(conf)
        .withType(schema)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        // no padding between row groups, it would only add bytes depending on the layout
        .withMaxPaddingSize(0);
    for (Map.Entry<String, String> option : options.entrySet()) {
      String value = option.getValue();
      switch (option.getKey()) {
        case "use_cdc":
          if (value.contains(",")) {
            String[] cdc = value.split(",");
            builder.withContentDefinedChunking(CdcOptions.builder()
                .withMinChunkSize(Long.parseLong(cdc[0]))
                .withMaxChunkSize(Long.parseLong(cdc[1]))
                .withNormLevel(Integer.parseInt(cdc[2]))
                .build());
          } else {
            builder.withContentDefinedChunking(Boolean.parseBoolean(value));
          }
          break;
        case "compression":
          builder.withCompressionCodec(
              value.equalsIgnoreCase("none")
                  ? CompressionCodecName.UNCOMPRESSED
                  : CompressionCodecName.valueOf(value.toUpperCase()));
          break;
        case "use_dictionary":
          builder.withDictionaryEncoding(Boolean.parseBoolean(value));
          break;
        case "row_group_size":
          // cut row groups by row count only, like the other writers
          builder.withRowGroupRowCountLimit(Integer.parseInt(value)).withRowGroupSize(Long.MAX_VALUE);
          break;
        case "data_page_size":
          builder.withPageSize(Integer.parseInt(value));
          break;
        case "max_rows_per_page":
          builder.withPageRowCountLimit(Integer.parseInt(value));
          break;
        default:
          throw new IllegalArgumentException("unknown option: " + option.getKey());
      }
    }

    try (ParquetReader<Group> reader =
            ParquetReader.builder(new GroupReadSupport(), src).withConf(conf).build();
        ParquetWriter<Group> writer = builder.build()) {
      for (Group group = reader.read(); group != null; group = reader.read()) {
        writer.write(group);
      }
    }
  }
}
