package com.fivetran.truffle;

import org.apache.parquet.column.ColumnReadStore;

abstract class ExprAssemble extends ExprBase {
    /**
     * Get ColumnReaders from ColumnReadStore
     */
    public abstract void prepare(ColumnReadStore readStore);

    public abstract long getTotalValueCount();
}
