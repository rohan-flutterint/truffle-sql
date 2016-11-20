package com.fivetran.truffle;

import org.apache.parquet.column.ColumnReadStore;

abstract class ExprAssemble extends ExprBase {
    /**
     * Get ColumnReaders from ColumnReadStore
     */
    abstract void prepare(ColumnReadStore readStore);

    abstract long getTotalValueCount();
}
