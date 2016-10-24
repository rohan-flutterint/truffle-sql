package com.fivetran.truffle.compiler;

import com.oracle.truffle.api.nodes.Node;

/**
 * Iterate over values in a column of a Dremel-style nested schema.
 * Very similar to {@link org.apache.parquet.column.ColumnReader}
 */
public abstract class Expr extends Node {

    /**
     * @return number of values in this column
     */
    public abstract long getTotalValueCount();

    /**
     * must return 0 when isFullyConsumed() == true
     * @return the repetition level for the current value
     */
    public abstract int getCurrentRepetitionLevel();

    /**
     * @return the definition level for the current value
     */
    public abstract int getCurrentDefinitionLevel();

    public abstract boolean isNull();

    public abstract CharSequence getString();

    public abstract boolean isString();

    /**
     * @return the current value
     */
    public abstract int getInteger();

    public abstract boolean isInteger();

    /**
     * @return the current value
     */
    public abstract boolean getBoolean();

    public abstract boolean isBoolean();

    /**
     * @return the current value
     */
    public abstract long getLong();

    public abstract boolean isLong();

    /**
     * @return the current value
     */
    public abstract float getFloat();

    public abstract boolean isFloat();

    /**
     * @return the current value
     */
    public abstract double getDouble();

    public abstract boolean isDouble();

    /**
     * This column is of type T and contains a small number of unique values.
     * A particular value is represented as an integer.
     * Values like this can be processed efficiently using perfect hash tables.
     */
    public abstract <T> int getDict(Class<T> type);

    public abstract <T> boolean isDict(Class<T> type);

    /**
     * Skip the current value
     */
    public abstract void skip();

    /**
     * @return If there are no remaining values
     */
    public abstract boolean isFullyConsumed();

    public abstract Object getObject();
}
