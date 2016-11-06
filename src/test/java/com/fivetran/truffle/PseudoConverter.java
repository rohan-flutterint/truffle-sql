package com.fivetran.truffle;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * Record converter that does nothing, so that we can access ColumnReader API directly.
 */
public class PseudoConverter extends GroupConverter {
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
