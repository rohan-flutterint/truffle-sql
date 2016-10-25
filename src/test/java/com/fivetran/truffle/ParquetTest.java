package com.fivetran.truffle;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.logging.Logger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class ParquetTest {
    private static final Logger LOG = Logger.getLogger(ParquetTest.class.getName());

    private static Schema idSchema = readSchema(ParquetTest.class.getResourceAsStream("/IdSchema.json"));
    private static Schema idNameSchema = readSchema(ParquetTest.class.getResourceAsStream("/IdNameSchema.json"));
    private static URI outputPath = tempFile();

    private static URI tempFile() {
        try {
            return Files.createTempFile("Output", ".parquet").toUri();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void writeParquet() throws IOException {
        // Not quite sure what this is
        GenericData model = GenericData.get();

        // Create writer
        int blockSize = 256 * 1024 * 1024;
        int pageSize = 64 * 1024;
        ParquetWriter<GenericRecord> parquetWriter = AvroParquetWriter
                .<GenericRecord>builder(new Path(outputPath))
                .withSchema(idNameSchema)
                .withDataModel(model)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(blockSize)
                .withPageSize(pageSize)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();

        parquetWriter.write(idName(1, "one"));
        parquetWriter.write(idName(2, "two"));
        parquetWriter.close();

        LOG.info("Wrote " + outputPath);
    }

    @Test
    public void projectColumn() throws IOException {
        // Set what columns we want to project from file
        Configuration conf = new Configuration();
        AvroReadSupport.setRequestedProjection(conf, idSchema);

        // Read records
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>();
        ParquetReader<GenericRecord> parquetReader = AvroParquetReader
                .builder(readSupport, new Path(outputPath))
                .withConf(conf)
                .build();

        // Check first record
        GenericRecord first = parquetReader.read();

        assertThat(first.get("id"), equalTo(1));
        assertThat(first.get("name"), nullValue());
    }

    @Test
    public void filterOnRead() throws IOException {
        // Filter out first row
        FilterPredicate filterPredicate = FilterApi.notEq(FilterApi.intColumn("id"), 1);

        // Read records
        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>();
        ParquetReader<GenericRecord> parquetReader = AvroParquetReader
                .builder(readSupport, new Path(outputPath))
                .withFilter(FilterCompat.get(filterPredicate))
                .build();

        // Check first record
        GenericRecord first = parquetReader.read();

        assertThat(first.get("id"), equalTo(2));
        assertThat(first.get("name"), equalTo("two"));
    }

    private static Schema readSchema(InputStream schemaStream) {
        try {
            return new Schema.Parser().parse(schemaStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static GenericRecord idName(int id, String name) {
        return new GenericRecordBuilder(idNameSchema).set("id", id).set("name", name).build();
    }
}
