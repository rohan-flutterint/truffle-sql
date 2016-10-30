package com.fivetran.truffle;

import com.oracle.truffle.api.dsl.Specialization;

public interface ExprBinaryMath {
    @Specialization
    default SqlNull leftNull(SqlNull left, Object right) {
        return SqlNull.INSTANCE;
    }

    @Specialization
    default SqlNull rightNull(Object left, SqlNull right) {
        return SqlNull.INSTANCE;
    }
}
