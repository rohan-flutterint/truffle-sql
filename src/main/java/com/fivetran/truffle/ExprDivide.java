package com.fivetran.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo(shortName = "/")
public abstract class ExprDivide extends ExprBinary {

    @Specialization
    protected long sub(long left, long right) {
        // Don't use ExactMath because we don't want to throw ArithmeticException
        return left / right;
    }

    @Specialization
    protected double sub(double left, double right) {
        return left / right;
    }
}
