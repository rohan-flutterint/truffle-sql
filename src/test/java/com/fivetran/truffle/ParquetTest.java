package com.fivetran.truffle;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
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
        List<Footer> footers = Parquets.footers(fromFile);

        return Lists.transform(footers, footer -> Parquets.columns(fromFile, schema, footer));
    }

}
