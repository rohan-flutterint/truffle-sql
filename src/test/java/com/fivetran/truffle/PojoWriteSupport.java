package com.fivetran.truffle;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Objects;

public class PojoWriteSupport<T> extends WriteSupport<T> {
    private final MessageType schema;
    private RecordConsumer out;

    public PojoWriteSupport(MessageType schema) {
        this.schema = schema;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        out = recordConsumer;
    }

    @Override
    public void write(T record) {
        Objects.requireNonNull(out, "prepareForWrite(RecordConsumer) was never called");
        Objects.requireNonNull(record, "Tried to write a null record");

        out.startMessage();

        try {
            writeFields(record);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        out.endMessage();
    }

    private void writeValue(Object value) throws IllegalAccessException {
        Objects.requireNonNull(value, "Cannot write null value, must omit the entire field");

        Class<?> type = value.getClass();

        if (type.isArray()) {
            int length = Array.getLength(value);

            for (int i = 0; i < length; i++) {
                Object element = Array.get(value, i);

                writeValue(element);
            }
        }
        else if (type == Boolean.class)
            out.addBoolean((boolean) value);
        else if (type == Integer.class)
            out.addInteger((int) value);
        else if (type == Long.class)
            out.addLong((long) value);
        else if (type == Float.class)
            out.addFloat((float) value);
        else if (type == Double.class)
            out.addDouble((double) value);
        else if (type == String.class)
            out.addBinary(Binary.fromString((String) value));
        else {
            out.startGroup();

            writeFields(value);

            out.endGroup();
        }
    }

    private void writeFields(Object record) throws IllegalAccessException {
        Class<?> type = record.getClass();

        Field[] fields = type.getFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            Object value = field.get(record);

            if (value != null && !isEmptyArray(value)) {
                out.startField(field.getName(), i);

                writeValue(value);

                out.endField(field.getName(), i);
            }
        }
    }

    private boolean isEmptyArray(Object value) {
        return value.getClass().isArray() && Array.getLength(value) == 0;
    }
}
