package com.fivetran.truffle;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Logger;

public final class ParquetTestResources {

    protected static final Logger LOG = Logger.getLogger(ParquetTestResources.class.getName());

    private static MessageType documentType;

    private static URI documentPath;

    public static URI documentPath() throws IOException {
        ensureDocument();

        return documentPath;
    }

    public static MessageType documentType() throws IOException {
        ensureDocument();

        return documentType;
    }

    /**
     * Document type from Dremel paper
     *
     * http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf
     */
    private static MessageType createDocumentType() {
        PrimitiveType forward = org.apache.parquet.schema.Types.repeated(PrimitiveType.PrimitiveTypeName.INT64).named("forward");
        PrimitiveType backward = org.apache.parquet.schema.Types.repeated(PrimitiveType.PrimitiveTypeName.INT64).named("backward");
        GroupType links = org.apache.parquet.schema.Types.buildGroup(Type.Repetition.OPTIONAL)
                .addField(backward)
                .addField(forward)
                .named("links");

        PrimitiveType code = org.apache.parquet.schema.Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named("code");
        PrimitiveType country = org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("country");
        GroupType language = org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REPEATED)
                .addField(code)
                .addField(country)
                .named("language");

        PrimitiveType url = org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("url");
        GroupType name = org.apache.parquet.schema.Types.buildGroup(Type.Repetition.REPEATED)
                .addField(language)
                .addField(url)
                .named("name");

        PrimitiveType docId = org.apache.parquet.schema.Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("docId");

        return org.apache.parquet.schema.Types.buildMessage()
                .addField(docId)
                .addField(links)
                .addField(name)
                .named("document");
    }

    private static void ensureDocument() throws IOException {
        if (documentPath != null)
            return;

        documentType = createDocumentType();
        documentPath = Paths.get("./target/generated-test-sources/Document.parquet").toUri();

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

        LOG.info("Wrote " + documentPath);
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
