package com.fivetran.truffle;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

/**
 * Writes Java POJOs to parquet using reflection.
 * You have to specify a MessageType that exactly matches the layout of the POJO fields.
 * Not fast! Only for testing.
 */
public class PojoParquetWriter<T> extends ParquetWriter<T> {
    public PojoParquetWriter(Path file, MessageType schema) throws IOException {
        super(file, new PojoWriteSupport<T>(schema));
    }
}
