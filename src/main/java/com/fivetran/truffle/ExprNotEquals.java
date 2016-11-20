package com.fivetran.truffle;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

import java.util.Objects;

@NodeInfo(shortName = "=")
abstract class ExprNotEquals extends ExprBinary {


    @Specialization
    protected boolean notEq(boolean left, boolean right) {
        return left != right;
    }

    @Specialization
    protected boolean notEq(long left, long right) {
        return left != right;
    }

    @Specialization
    protected boolean notEq(double left, double right) {
        return left != right;
    }

    @Specialization
    protected SqlNull notEq(SqlNull left, Object right) {
        return SqlNull.INSTANCE;
    }

    @Specialization
    protected SqlNull notEq(Object left, SqlNull right) {
        return SqlNull.INSTANCE;
    }

    @Specialization
    protected boolean notEq(String left, String right) {
        return !Objects.equals(left, right);
    }

    @Specialization
    @CompilerDirectives.TruffleBoundary
    protected boolean notEq(Object left, Object right) {
        return left != right;
    }
}
