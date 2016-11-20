package com.fivetran.truffle.compile;

import org.apache.parquet.column.ColumnReadStore;

abstract class ExprAssemble extends ExprBase {
    /**
     * Get ColumnReaders from ColumnReadStore
     */
    abstract void prepare(ColumnReadStore readStore);

    abstract long getTotalValueCount();
}
