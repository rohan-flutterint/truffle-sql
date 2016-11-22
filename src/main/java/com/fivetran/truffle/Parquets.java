package com.fivetran.truffle;

import com.oracle.truffle.api.object.Shape;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Parquets {
    private static final Configuration conf = new Configuration();

    public static ColumnReadStore columns(URI fromFile, MessageType schema, Footer footer) {
        try {
            List<BlockMetaData> filteredBlocks = blockMetaData(footer);

            // Create file-reader
            FileMetaData parquetFileMetadata = footer.getParquetMetadata().getFileMetaData();
            ParquetFileReader fileReader = new ParquetFileReader(
                    conf,
                    parquetFileMetadata,
                    new Path(fromFile),
                    filteredBlocks,
                    schema.getColumns()
            );
            PageReadStore pageReadStore = fileReader.readNextRowGroup();

            // Dummy converter - we will never actually assemble the records
            GroupConverter recordConverter = new PseudoConverter();

            // Get reader for columns
            return new ColumnReadStoreImpl(pageReadStore, recordConverter, schema, "test");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<BlockMetaData> blockMetaData(Footer footer) {
        // Determine which blocks to read
        List<BlockMetaData> blocks = footer.getParquetMetadata().getBlocks();
        MessageType fileSchema = footer.getParquetMetadata().getFileMetaData().getSchema();

        return RowGroupFilter.filterRowGroups(FilterCompat.NOOP, blocks, fileSchema);
    }

    public static List<Footer> footers(URI fromFile) {
        try {
            Path file = new Path(fromFile);
            FileSystem fs = file.getFileSystem(conf);
            List<FileStatus> statuses = Arrays.asList(fs.listStatus(file, HiddenFileFilter.INSTANCE));

            return ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static RelDataType sqlType(MessageType parquetType, RelDataTypeFactory typeFactory) {
        return doSqlType(parquetType, typeFactory);
    }

    private static RelDataType doSqlType(Type parquetType, RelDataTypeFactory typeFactory) {
        if (parquetType.isPrimitive()) {
            PrimitiveType.PrimitiveTypeName primitive = parquetType.asPrimitiveType().getPrimitiveTypeName();

            return typeFactory.createSqlType(sqlPrimitive(primitive));
        }
        else {
            GroupType group = parquetType.asGroupType();
            RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();

            for (Type field : group.getFields()) {
                builder.add(field.getName(), doSqlType(field, typeFactory));
            }

            return builder.build();
        }
    }

    private static SqlTypeName sqlPrimitive(PrimitiveType.PrimitiveTypeName parquetType) {
        switch (parquetType) {
            case INT64:
            case INT32:
                return SqlTypeName.BIGINT;
            case BOOLEAN:
                return SqlTypeName.BOOLEAN;
            case BINARY:
                return SqlTypeName.VARCHAR;
            case FLOAT:
            case DOUBLE:
                return SqlTypeName.DOUBLE;
            case INT96:
            case FIXED_LEN_BYTE_ARRAY:
            default:
                throw new RuntimeException(parquetType + " is not supported");
        }
    }

    public static Shape shape(GroupType schema) {
        throw new UnsupportedOperationException();
    }

    /**
     * Same as {@link Parquets#shape(GroupType)}, but optimistically assumes all fields will never be null.
     */
    public static Shape shapeOptimistic(GroupType schema) {
        throw new UnsupportedOperationException();
    }

    public static boolean containsPath(String[] parent, String[] child) {
        if (parent.length > child.length)
            return false;

        for (int i = 0; i < parent.length; i++) {
            if (!Objects.equals(parent[i], child[i]))
                return false;
        }

        return true;
    }

    /**
     * Record converter that does nothing, so that we can access ColumnReader API directly.
     */
    private static class PseudoConverter extends GroupConverter {
        @Override
        public Converter getConverter(int fieldIndex) {
            return new PseudoConverter();
        }

        @Override
        public void start() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void end() {
            throw new UnsupportedOperationException();
        }

        @Override
        public PrimitiveConverter asPrimitiveConverter() {
            return new PrimitiveConverter() {
                @Override
                public GroupConverter asGroupConverter() {
                    return new PseudoConverter();
                }
            };
        }
    }
}
