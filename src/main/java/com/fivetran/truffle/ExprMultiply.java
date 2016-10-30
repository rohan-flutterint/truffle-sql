package com.fivetran.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo(shortName = "*")
public abstract class ExprMultiply extends ExprBinary implements ExprBinaryMath {

    @Specialization
    protected long mul(long left, long right) {
        // Don't use ExactMath because we don't want to throw ArithmeticException
        return left * right;
    }

    @Specialization
    protected double mul(double left, double right) {
        return left * right;
    }
}
