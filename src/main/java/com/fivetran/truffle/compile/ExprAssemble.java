package com.fivetran.truffle.compile;

import org.apache.parquet.column.ColumnReadStore;

/**
 * Base of expressions that read a column (primitive or nested) from somewhere (probably a Parquet file).
 *
 * This is a bit of an unconventional Truffle expression - the method prepare(ColumnReadStore) installs ColumnReaders
 * that are used by execute*(VirtualFrame) methods.
 */
abstract class ExprAssemble extends ExprBase {
    /**
     * Install ColumnReaders from ColumnReadStore
     */
    abstract void prepare(ColumnReadStore readStore);
}
