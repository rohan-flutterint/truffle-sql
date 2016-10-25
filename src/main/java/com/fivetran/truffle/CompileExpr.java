package com.fivetran.truffle;

import org.apache.calcite.rex.*;

import javax.annotation.Nullable;
import java.util.Objects;

public class CompileExpr implements RexVisitor<XIterator> {
    @Nullable
    private final XRel input;

    public CompileExpr(XRel input) {
        this.input = input;
    }


    @Override
    public XIterator visitInputRef(RexInputRef inputRef) {
        Objects.requireNonNull(input);

        return input.get(inputRef.getIndex());
    }

    @Override
    public XIterator visitLocalRef(RexLocalRef localRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public XIterator visitLiteral(RexLiteral literal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public XIterator visitCall(RexCall call) {
        throw new UnsupportedOperationException();
    }

    @Override
    public XIterator visitOver(RexOver over) {
        throw new UnsupportedOperationException();
    }

    @Override
    public XIterator visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public XIterator visitDynamicParam(RexDynamicParam dynamicParam) {
        throw new UnsupportedOperationException();
    }

    @Override
    public XIterator visitRangeRef(RexRangeRef rangeRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public XIterator visitFieldAccess(RexFieldAccess fieldAccess) {
        throw new UnsupportedOperationException();
    }

    @Override
    public XIterator visitSubQuery(RexSubQuery subQuery) {
        throw new UnsupportedOperationException();
    }
}
