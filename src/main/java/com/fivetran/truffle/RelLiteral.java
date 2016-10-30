package com.fivetran.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.*;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.SourceSection;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.List;

class RelLiteral extends RowSource {
    private final LogicalValues values;
    private final FrameDescriptor resultType;

    public RelLiteral(SourceSection source, LogicalValues values, RootNode then) {
        super(source, then);

        this.values = values;
        this.resultType = CompileRel.frame(values.getRowType());
    }

    @Override
    public Object execute(VirtualFrame frame) {
        assert frame.getFrameDescriptor().getSize() == 0 : "Input to literal should be empty but was " + frame.getFrameDescriptor();

        VirtualFrame thenFrame = Truffle.getRuntime().createVirtualFrame(new Object[]{}, resultType);
        List<? extends FrameSlot> slots = resultType.getSlots();

        for (List<RexLiteral> literals : values.getTuples()) {
            for (int i = 0; i < literals.size(); i++) {
                RexLiteral literal = literals.get(i);
                Object value = asObject(literal);
                FrameSlot slot = slots.get(i);

                thenFrame.setObject(slot, value);
            }

            then.execute(thenFrame);
        }

        return QueryReturn.INSTANCE;
    }

    static Object asObject(RexLiteral literal) {
        Object value = literal.getValue();

        switch (literal.getType().getSqlTypeName()) {
            case BOOLEAN:
                return (Boolean) value;
            case TINYINT:
            case SMALLINT:
                return ((Number) value).shortValue();
            case INTEGER:
                return ((Number) value).intValue();
            case BIGINT:
                return ((Number) value).longValue();
            case DECIMAL:
                return ((BigDecimal) value);
            case FLOAT:
                return ((Number) value).floatValue();
            case REAL:
                return ((Number) value).doubleValue();
            case DOUBLE:
                return ((Number) value).doubleValue();
            case DATE:
                return ((Calendar) value).toInstant().atOffset(ZoneOffset.UTC).toLocalDate();
            case TIME:
                return ((Calendar) value).toInstant().atOffset(ZoneOffset.UTC).toLocalTime();
            case TIMESTAMP:
                ((Calendar) value).toInstant();
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                throw new UnsupportedOperationException();
            case CHAR:
            case VARCHAR:
                return ((NlsString) value).getValue();
            case BINARY:
            case VARBINARY:
                throw new UnsupportedOperationException();
            case NULL:
                return NullValue.INSTANCE;
            case ANY:
            case SYMBOL:
            case MULTISET:
            case ARRAY:
            case MAP:
            case DISTINCT:
            case STRUCTURED:
            case ROW:
            case OTHER:
            case CURSOR:
            case COLUMN_LIST:
            case DYNAMIC_STAR:
            default:
                throw new UnsupportedOperationException();
        }
    }
}
