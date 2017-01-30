package com.fivetran.truffle.compile;

import com.oracle.truffle.api.object.DynamicObject;

/**
 * Generally, we try to compile queries into fully pipelined programs and avoid the 'iterator model'.
 * However, some operations like external sort produce iterators that are consumed by the next pipelined stage.
 */
public interface RowIterator {
    /**
     * @return Next row, or SqlNull.INSTANCE if there are no more rows
     */
    DynamicObject next();
}
