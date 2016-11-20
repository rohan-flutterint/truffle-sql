package com.fivetran.truffle.compile;

import org.apache.parquet.column.ColumnReadStore;

/**
 * Base of expressions that read a column (primitive or nested) from somewhere (probably a Parquet file).
 */
abstract class ExprAssemble extends ExprBase {
    /**
     * Get ColumnReaders from ColumnReadStore
     */
    abstract void prepare(ColumnReadStore readStore);

    abstract long getTotalValueCount();
}
