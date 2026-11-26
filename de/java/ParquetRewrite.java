// Rewrite parquet files with parquet-java, used by de.formats.ParquetJava.
//
// usage: java -cp <parquet-hadoop classpath> ParquetRewrite.java < JOBS
//
// Every line of JOBS is a tab separated SRC, DEST and KEY=VALUE options, and every job
// prints a line to stdout: "ok\tDEST" or "error\tDEST\tMESSAGE". A failing job doesn't
// stop the others, so a single JVM writes them all.
//
// The options mirror de.formats.Parquet: use_cdc=false|true|MIN,MAX,NORM,
// compression, use_dictionary, row_group_size (rows), data_page_size (bytes),
// max_rows_per_page and data_page_version=1.0|2.0 (the writer version). Options left
// out keep the parquet-java defaults.

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.parquet.column.ContentDefinedChunkingOptions;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
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
    BufferedReader jobs =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    for (String line = jobs.readLine(); line != null; line = jobs.readLine()) {
      String[] fields = line.split("\t");
      Map<String, String> options = new LinkedHashMap<>();
      for (int i = 2; i < fields.length; i++) {
        String[] option = fields[i].split("=", 2);
        options.put(option[0], option[1]);
      }
      try {
        rewrite(new Path(fields[0]), new Path(fields[1]), options);
        System.out.println("ok\t" + fields[1]);
      } catch (Exception e) {
        System.out.println("error\t" + fields[1] + "\t" + e.toString().replace('\n', ' '));
      }
      System.out.flush();
    }
  }

  static void rewrite(Path src, Path dest, Map<String, String> options) throws Exception {
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
        .withMaxPaddingSize(0)
        // no CRC in the page headers, like parquet-cpp and parquet-rs, so the same
        // pages can have the same bytes
        .withPageWriteChecksumEnabled(false);
    for (Map.Entry<String, String> option : options.entrySet()) {
      String value = option.getValue();
      switch (option.getKey()) {
        case "use_cdc":
          if (value.contains(",")) {
            String[] cdc = value.split(",");
            builder.withContentDefinedChunking(new ContentDefinedChunkingOptions(
                Long.parseLong(cdc[0]), Long.parseLong(cdc[1]), Integer.parseInt(cdc[2])));
          } else {
            builder.withContentDefinedChunking(
                Boolean.parseBoolean(value) ? ContentDefinedChunkingOptions.DEFAULT : null);
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
        case "data_page_version":
          switch (value) {
            case "1.0":
              builder.withWriterVersion(WriterVersion.PARQUET_1_0);
              break;
            case "2.0":
              builder.withWriterVersion(WriterVersion.PARQUET_2_0);
              break;
            default:
              throw new IllegalArgumentException("unknown data page version: " + value);
          }
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
