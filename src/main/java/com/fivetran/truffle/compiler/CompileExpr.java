package com.fivetran.truffle.compiler;

import org.apache.calcite.rex.*;

import javax.annotation.Nullable;
import java.util.Objects;

public class CompileExpr implements RexVisitor<Expr> {
    @Nullable
    private final Rel input;

    public CompileExpr(Rel input) {
        this.input = input;
    }


    @Override
    public Expr visitInputRef(RexInputRef inputRef) {
        Objects.requireNonNull(input);

        return input.get(inputRef.getIndex());
    }

    @Override
    public Expr visitLocalRef(RexLocalRef localRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expr visitLiteral(RexLiteral literal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expr visitCall(RexCall call) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expr visitOver(RexOver over) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expr visitCorrelVariable(RexCorrelVariable correlVariable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expr visitDynamicParam(RexDynamicParam dynamicParam) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expr visitRangeRef(RexRangeRef rangeRef) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expr visitFieldAccess(RexFieldAccess fieldAccess) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expr visitSubQuery(RexSubQuery subQuery) {
        throw new UnsupportedOperationException();
    }
}
