package com.fivetran.truffle.compile;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;

@NodeChild("target")
abstract class ExprIsNull extends ExprBase {
    @Specialization
    boolean executeNull(SqlNull isNull) {
        return true;
    }

    @Specialization
    boolean executeObject(Object value) {
        return false;
    }

    // TODO consider adding additional specializations
}
