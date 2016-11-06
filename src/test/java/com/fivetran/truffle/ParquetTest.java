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
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.Types;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Logger;

public class ParquetTest {
    private static final Logger LOG = Logger.getLogger(ParquetTest.class.getName());

    private static MessageType documentType = createDocumentType();

    /**
     * Document type from Dremel paper
     *
     * http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf
     */
    private static MessageType createDocumentType() {
        PrimitiveType forward = Types.repeated(PrimitiveType.PrimitiveTypeName.INT64).named("forward");
        PrimitiveType backward = Types.repeated(PrimitiveType.PrimitiveTypeName.INT64).named("backward");
        GroupType links = Types.buildGroup(Type.Repetition.OPTIONAL)
                .addField(backward)
                .addField(forward)
                .named("links");

        PrimitiveType code = Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named("code");
        PrimitiveType country = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("country");
        GroupType language = Types.buildGroup(Type.Repetition.REPEATED)
                .addField(code)
                .addField(country)
                .named("language");

        PrimitiveType url = Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("url");
        GroupType name = Types.buildGroup(Type.Repetition.REPEATED)
                .addField(language)
                .addField(url)
                .named("name");

        PrimitiveType docId = Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("docId");

        return Types.buildMessage()
                .addField(docId)
                .addField(links)
                .addField(name)
                .named("document");
    }

    private static URI documentPath = Paths.get("./target/generated-test-sources/Document.parquet").toUri();

    @BeforeClass
    public static void writeDocument() throws IOException {
        ParquetWriter<Document> parquetWriter = pojoWriter(documentPath, Document.class, documentType);

        parquetWriter.write(document(
                10,
                links(
                        repeatedLong(),
                        repeatedLong(20L, 40L, 60L)
                ),
                repeated(
                        name(
                                repeated(
                                        language("en-us", "us"),
                                        language("en", null)
                                ),
                                "http://A"
                        ),
                        name(
                                repeated(new Language[0]),
                                "http://B"
                        ),
                        name(
                                repeated(
                                        language("en-gb", "gb")
                                ),
                                null
                        )
                )
        ));
        parquetWriter.write(document(
                20,
                links(
                        repeatedLong(10, 30),
                        repeatedLong(80)
                ),
                repeated(
                        name(
                                repeated(new Language[0]),
                                "http://C"
                        )
                )
        ));

        parquetWriter.close();

        LOG.info("Wrote " + ParquetTest.documentPath);
    }

    @Test
    public void readDocumentColumns() throws IOException {
        ColumnDescriptor docId = documentType.getColumnDescription(new String[]{"docId"});
        ColumnDescriptor linksForward = documentType.getColumnDescription(new String[]{"links", "forward"});
        ColumnDescriptor nameLanguageCountry = documentType.getColumnDescription(new String[]{"name", "language", "country"});

        for (ColumnReadStore page : readColumns(documentPath, documentType)) {
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

    private static class Document {
        public final long docId;
        public final Links links;
        public final Name[] name;

        private Document(long docId, Links links, Name[] name) {
            this.docId = docId;
            this.links = links;
            this.name = name;
        }
    }

    public static class Links {
        public final long[] backward, forward;

        public Links(long[] backward, long[] forward) {
            this.backward = backward;
            this.forward = forward;
        }
    }

    public static class Name {
        public final Language[] language;
        public final String url;

        public Name(Language[] language, String url) {
            this.language = language;
            this.url = url;
        }
    }

    public static class Language {
        public final String code, country;

        public Language(String code, String country) {
            this.code = code;
            this.country = country;
        }
    }

    private static Document document(long docId, Links links, Name[] name) {
        return new Document(docId, links, name);
    }

    private static Links links(long[] backward, long[] forward) {
        return new Links(backward, forward);
    }

    private static long[] repeatedLong(long... values) {
        return values;
    }

    private static <T> T[] repeated(T... records) {
        return records;
    }

    private static Name name(Language[] language, String url) {
        return new Name(language, url);
    }

    private static Language language(String code, String country) {
        return new Language(code, country);
    }

    private static <T> ParquetWriter<T> pojoWriter(URI file, Class<T> clazz, MessageType schema) throws IOException {
        Files.deleteIfExists(Paths.get(file));

        return new PojoParquetWriter<>(new Path(file), schema);
    }
}
