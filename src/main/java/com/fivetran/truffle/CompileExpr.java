package com.fivetran.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

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
        if (RexLiteral.isNullLiteral(literal))
            return ExprLiteral.Null();

        Object value = Types.coerceLiteral(literal);
        SqlTypeName type = literal.getType().getSqlTypeName();
        FrameSlotKind kind = Types.kind(type);

        switch (kind) {
            case Long:
                return ExprLiteral.Long((long) value);
            case Double:
                return ExprLiteral.Double((double) value);
            case Boolean:
                return ExprLiteral.Boolean((boolean) value);
            case Object:
                return ExprLiteral.Object(value);
            case Illegal:
            default:
                throw new RuntimeException("Don't know what to do with " + literal);
        }
    }

    @Override
    public ExprBase visitCall(RexCall call) {
        switch (call.getKind()) {
            case TIMES:
                return binary(call, ExprMultiplyNodeGen::create);
            case DIVIDE:
                return binary(call, ExprDivideNodeGen::create);
            case PLUS:
                return binary(call, ExprPlusNodeGen::create);
            case MINUS:
                return binary(call, ExprMinusNodeGen::create);
            case IN:
                throw new UnsupportedOperationException();
            case LESS_THAN:
                throw new UnsupportedOperationException();
            case GREATER_THAN:
                throw new UnsupportedOperationException();
            case LESS_THAN_OR_EQUAL:
                throw new UnsupportedOperationException();
            case GREATER_THAN_OR_EQUAL:
                throw new UnsupportedOperationException();
            case EQUALS:
                return binary(call, ExprEqualsNodeGen::create);
            case NOT_EQUALS:
                return binary(call, ExprNotEqualsNodeGen::create);
            case OR:
                return binary(call, ExprOrNodeGen::create);
            case AND:
                return binary(call, ExprAndNodeGen::create);
            case LIKE:
                throw new UnsupportedOperationException();
            case SIMILAR:
                throw new UnsupportedOperationException();
            case BETWEEN:
                throw new UnsupportedOperationException();
            case CASE:
                throw new UnsupportedOperationException();
            case NULLIF:
                throw new UnsupportedOperationException();
            case COALESCE:
                throw new UnsupportedOperationException();
            case TIMESTAMP_ADD:
                throw new UnsupportedOperationException();
            case TIMESTAMP_DIFF:
                throw new UnsupportedOperationException();
            case NOT:
                throw new UnsupportedOperationException();
            case PLUS_PREFIX:
                throw new UnsupportedOperationException();
            case MINUS_PREFIX:
                throw new UnsupportedOperationException();
            case IS_NULL:
                throw new UnsupportedOperationException();
            case IS_NOT_NULL:
                throw new UnsupportedOperationException();
            case ROW:
                throw new UnsupportedOperationException();
            case CAST:
                throw new UnsupportedOperationException();
            case FLOOR:
                throw new UnsupportedOperationException();
            case CEIL:
                throw new UnsupportedOperationException();
            case TRIM:
                throw new UnsupportedOperationException();
            case LTRIM:
                throw new UnsupportedOperationException();
            case RTRIM:
                throw new UnsupportedOperationException();
            case EXTRACT:
                throw new UnsupportedOperationException();
            case UNNEST:
                throw new UnsupportedOperationException();
            case COUNT:
                throw new UnsupportedOperationException();
            case SUM:
                throw new UnsupportedOperationException();
            case MIN:
                throw new UnsupportedOperationException();
            case MAX:
                throw new UnsupportedOperationException();
            case LEAD:
                throw new UnsupportedOperationException();
            case LAG:
                throw new UnsupportedOperationException();
            case FIRST_VALUE:
                throw new UnsupportedOperationException();
            case LAST_VALUE:
                throw new UnsupportedOperationException();
            case COVAR_POP:
                throw new UnsupportedOperationException();
            case COVAR_SAMP:
                throw new UnsupportedOperationException();
            case AVG:
                throw new UnsupportedOperationException();
            case STDDEV_POP:
                throw new UnsupportedOperationException();
            case STDDEV_SAMP:
                throw new UnsupportedOperationException();
            case VAR_POP:
                throw new UnsupportedOperationException();
            case VAR_SAMP:
                throw new UnsupportedOperationException();
            case NTILE:
                throw new UnsupportedOperationException();
            case ROW_NUMBER:
                throw new UnsupportedOperationException();
            case RANK:
                throw new UnsupportedOperationException();
            case PERCENT_RANK:
                throw new UnsupportedOperationException();
            case DENSE_RANK:
                throw new UnsupportedOperationException();
            case CUME_DIST:
                throw new UnsupportedOperationException();
            default:
                throw new RuntimeException("Don't know what to do with " + call.getKind());
        }
    }

    @FunctionalInterface
    private interface BinaryConstructor {
        ExprBinary accept(ExprBase left, ExprBase right);
    }

    private ExprBase binary(RexCall call, BinaryConstructor then) {
        List<RexNode> operands = call.getOperands();

        assert operands.size() == 2;

        ExprBase left = operands.get(0).accept(new CompileExpr(from));
        ExprBase right = operands.get(1).accept(new CompileExpr(from));

        return then.accept(left, right);
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
        ExprBase receiver = fieldAccess.getReferenceExpr().accept(new CompileExpr(from));
        String name = fieldAccess.getField().getName();

        return ExprReadPropertyNodeGen.create(name, receiver);
    }

    @Override
    public ExprBase visitSubQuery(RexSubQuery subQuery) {
        throw new UnsupportedOperationException();
    }
}
