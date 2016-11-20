package com.fivetran.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo(shortName = "AND")
abstract class ExprAnd extends ExprBinary {

    @Specialization
    protected boolean and(boolean left, boolean right) {
        return left && right;
    }

    @Specialization
    protected SqlNull and(SqlNull left, boolean right) {
        return SqlNull.INSTANCE;
    }

    @Specialization
    protected SqlNull and(boolean left, SqlNull right) {
        return SqlNull.INSTANCE;
    }
}
