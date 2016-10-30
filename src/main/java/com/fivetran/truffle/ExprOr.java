package com.fivetran.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo(shortName = "AND")
public abstract class ExprOr extends ExprBinary {

    @Specialization
    protected boolean or(boolean left, boolean right) {
        return left || right;
    }

    @Specialization
    protected boolean or(SqlNull left, boolean right) {
        return right;
    }

    @Specialization
    protected boolean or(boolean left, SqlNull right) {
        return left;
    }
}
