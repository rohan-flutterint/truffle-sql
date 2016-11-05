package com.fivetran.truffle;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class ParquetTest {
    private static final Logger LOG = Logger.getLogger(ParquetTest.class.getName());

    public static Schema idSchema = readSchema(ParquetTest.class.getResourceAsStream("/IdSchema.json"));
    public static Schema idNameSchema = readSchema(ParquetTest.class.getResourceAsStream("/IdNameSchema.json"));
    public static URI outputPath = tempFile();

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

    @Test
    public void columnReader() throws IOException {
        Configuration conf = new Configuration();

        // Open file, read 1 footer
        Path file = new Path(outputPath);
        FileSystem fs = file.getFileSystem(conf);
        List<FileStatus> statuses = Arrays.asList(fs.listStatus(file, HiddenFileFilter.INSTANCE));
        List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);
        Footer footer = footers.get(0); // TODO not good!
        FileMetaData parquetFileMetadata = footer.getParquetMetadata().getFileMetaData();

        // Determine which blocks to read
        List<BlockMetaData> blocks = footer.getParquetMetadata().getBlocks();
        MessageType fileSchema = footer.getParquetMetadata().getFileMetaData().getSchema();
        List<BlockMetaData> filteredBlocks = RowGroupFilter.filterRowGroups(
                FilterCompat.NOOP, blocks, fileSchema);

        // Determine which columns to read
        MessageType schema = new AvroSchemaConverter().convert(idNameSchema);
        List<ColumnDescriptor> columns = schema.getColumns();

        // Create file-reader
        ParquetFileReader fileReader = new ParquetFileReader(conf, parquetFileMetadata, file, filteredBlocks, columns); // TODO
        PageReadStore pageReadStore = fileReader.readNextRowGroup();
        // Dummy converter - we will never actually assemble the records
        GroupConverter recordConverter = new GroupConverter() {
            @Override
            public Converter getConverter(int fieldIndex) {
                return new PrimitiveConverter() {
                };
            }

            @Override
            public void start() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void end() {
                throw new UnsupportedOperationException();
            }
        };

        // Get reader for columns
        ColumnReadStoreImpl columnReadStore = new ColumnReadStoreImpl(pageReadStore, recordConverter, schema, "test");

        // Column id
        ColumnDescriptor idColumn = columns.get(0);

        assertThat(idColumn.getType(), equalTo(PrimitiveType.PrimitiveTypeName.INT32));

        int maxDefinition = idColumn.getMaxDefinitionLevel();
        int maxRepetition = idColumn.getMaxRepetitionLevel();

        ColumnReader columnReader = columnReadStore.getColumnReader(idColumn);

        // Row 1
        int id = columnReader.getInteger();
        int definition = columnReader.getCurrentDefinitionLevel();
        int repetition = columnReader.getCurrentRepetitionLevel();

        System.out.println(definition + "/" + maxDefinition + ", " + repetition + "/" + maxRepetition + ", " + id);

        // Row 2
        columnReader.consume();

        id = columnReader.getInteger();
        definition = columnReader.getCurrentDefinitionLevel();
        repetition = columnReader.getCurrentRepetitionLevel();

        System.out.println(definition + "/" + maxDefinition + ", " + repetition + "/" + maxRepetition + ", " + id);

        // Column attr
        ColumnDescriptor attrColumn = columns.get(1);

        assertThat(attrColumn.getType(), equalTo(PrimitiveType.PrimitiveTypeName.BINARY));

        maxDefinition = attrColumn.getMaxDefinitionLevel();
        maxRepetition = attrColumn.getMaxRepetitionLevel();

        columnReader = columnReadStore.getColumnReader(attrColumn);

        // Row 1
        String attr = columnReader.getBinary().toStringUsingUTF8();
        definition = columnReader.getCurrentDefinitionLevel();
        repetition = columnReader.getCurrentRepetitionLevel();

        System.out.println(definition + "/" + maxDefinition + ", " + repetition + "/" + maxRepetition + ", " + attr);

        // Row 2
        columnReader.consume();

        attr = columnReader.getBinary().toStringUsingUTF8();
        definition = columnReader.getCurrentDefinitionLevel();
        repetition = columnReader.getCurrentRepetitionLevel();

        System.out.println(definition + "/" + maxDefinition + ", " + repetition + "/" + maxRepetition + ", " + attr);
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
