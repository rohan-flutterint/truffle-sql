package com.fivetran.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import org.apache.calcite.rex.*;

class CompileExpr implements RexVisitor<ExprBase> {
    /**
     * FROM clause of SQL query.
     *
     * Can be empty in queries like SELECT 1
     */
    private final FrameDescriptor from;

    CompileExpr(FrameDescriptor from) {
        this.from = from;
    }

    @Override
    public ExprBase visitInputRef(RexInputRef inputRef) {
        FrameSlot slot = from.findFrameSlot(inputRef.getIndex());

        return new ExprProject(slot);
    }

    @Override
    public ExprBase visitLocalRef(RexLocalRef localRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExprBase visitLiteral(RexLiteral literal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExprBase visitCall(RexCall call) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExprBase visitOver(RexOver over) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExprBase visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExprBase visitDynamicParam(RexDynamicParam dynamicParam) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExprBase visitRangeRef(RexRangeRef rangeRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExprBase visitFieldAccess(RexFieldAccess fieldAccess) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExprBase visitSubQuery(RexSubQuery subQuery) {
        throw new UnsupportedOperationException();
    }
}
