package com.fivetran.truffle.compile;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;

@NodeChild("target")
abstract class ExprNot extends ExprBase {
    @Specialization
    boolean executeBoolean(boolean value) {
        return !value;
    }

    @Specialization
    SqlNull executeNull(SqlNull value) {
        return SqlNull.INSTANCE;
    }

    @Specialization
    boolean executeGeneric(Object value) {
        return false;
    }
}
