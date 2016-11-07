package com.fivetran.truffle;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static com.fivetran.truffle.ParquetTestResources.documentPath;
import static com.fivetran.truffle.ParquetTestResources.documentType;

public class ParquetTest {

    @Test
    public void readDocumentColumns() throws IOException {
        ColumnDescriptor docId = documentType().getColumnDescription(new String[]{"docId"});
        ColumnDescriptor linksForward = documentType().getColumnDescription(new String[]{"links", "forward"});
        ColumnDescriptor nameLanguageCountry = documentType().getColumnDescription(new String[]{"name", "language", "country"});

        for (ColumnReadStore page : readColumns(documentPath(), documentType())) {
            readColumn(page, docId, column -> column.getLong());
            readColumn(page, linksForward, column -> column.getLong());
            readColumn(page, nameLanguageCountry, column -> column.getBinary().toStringUsingUTF8());
        }
    }

    private static <T> void readColumn(ColumnReadStore read, ColumnDescriptor column, Function<ColumnReader, T> readValue) {
        int maxDefinition = column.getMaxDefinitionLevel();
        int maxRepetition = column.getMaxRepetitionLevel();

        System.out.println(Joiner.on(".").join(column.getPath()));
        System.out.println("v\tr" + maxRepetition + "\td" + maxDefinition);

        ColumnReader columnReader = read.getColumnReader(column);
        long count = columnReader.getTotalValueCount();

        for (long row = 0; row < count; row++) {
            int definition = columnReader.getCurrentDefinitionLevel();
            int repetition = columnReader.getCurrentRepetitionLevel();
            String value = definition < maxDefinition ? "-" : readValue.apply(columnReader).toString();

            System.out.println(value + "\t" + repetition + "\t" + definition);

            columnReader.consume();
        }

        System.out.println();
    }

    private static Iterable<? extends ColumnReadStore> readColumns(URI fromFile, MessageType schema) throws IOException {
        Path file = new Path(fromFile);
        Configuration conf = new Configuration();
        FileSystem fs = file.getFileSystem(conf);
        List<FileStatus> statuses = Arrays.asList(fs.listStatus(file, HiddenFileFilter.INSTANCE));
        List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);

        return Lists.transform(footers, footer -> {
            try {
                // Determine which blocks to read
                List<BlockMetaData> blocks = footer.getParquetMetadata().getBlocks();
                MessageType fileSchema = footer.getParquetMetadata().getFileMetaData().getSchema();
                List<BlockMetaData> filteredBlocks = RowGroupFilter.filterRowGroups(
                        FilterCompat.NOOP, blocks, fileSchema);

                // Create file-reader
                FileMetaData parquetFileMetadata = footer.getParquetMetadata().getFileMetaData();
                ParquetFileReader fileReader = new ParquetFileReader(conf, parquetFileMetadata, file, filteredBlocks, schema.getColumns());
                PageReadStore pageReadStore = fileReader.readNextRowGroup();

                // Dummy converter - we will never actually assemble the records
                GroupConverter recordConverter = new PseudoConverter();

                // Get reader for columns
                return new ColumnReadStoreImpl(pageReadStore, recordConverter, schema, "test");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
