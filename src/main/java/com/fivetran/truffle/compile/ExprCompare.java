package com.fivetran.truffle.compile;

import com.oracle.truffle.api.dsl.Specialization;

/**
 * Expression used in sorting. Compares two child expressions and produces 1, -1, or 0.
 */
abstract class ExprCompare extends ExprBinary {

    private final boolean nullsAreLargest, ascending;

    ExprCompare(boolean nullsAreLargest, boolean ascending) {
        this.nullsAreLargest = nullsAreLargest;
        this.ascending = ascending;
    }

    @Specialization
    protected long compare(long left, long right) {
        if (ascending)
            return Long.compare(left, right);
        else
            return Long.compare(right, left);
    }

    @Specialization
    protected long compare(double left, double right) {
        if (ascending)
            return Double.compare(left, right);
        else
            return Double.compare(right, left);
    }

    @Specialization
    protected long bothNull(SqlNull left, SqlNull right) {
        return 0;
    }

    @Specialization
    protected long leftNull(SqlNull left, Object right) {
        return nullsAreLargest ? 1 : -1;
    }

    @Specialization
    protected long rightNull(Object left, SqlNull right) {
        return nullsAreLargest ? -1 : 1;
    }

}
