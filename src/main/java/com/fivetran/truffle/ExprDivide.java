package com.fivetran.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo(shortName = "/")
public abstract class ExprDivide extends ExprBinary {

    @Specialization
    protected long div(long left, long right) {
        // Don't use ExactMath because we don't want to throw ArithmeticException
        return left / right;
    }

    @Specialization
    protected double div(double left, double right) {
        return left / right;
    }

    @Specialization
    protected SqlNull leftNull(SqlNull left, Object right) {
        return SqlNull.INSTANCE;
    }

    @Specialization
    protected SqlNull rightNull(Object left, SqlNull right) {
        return SqlNull.INSTANCE;
    }
}
