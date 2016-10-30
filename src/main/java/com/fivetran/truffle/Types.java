package com.fivetran.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.List;

public class Types {
    static FrameDescriptor frame(RelDataType rowType) {
        List<RelDataTypeField> fields = rowType.getFieldList();
        FrameDescriptor describe = new FrameDescriptor();

        for (int column = 0; column < fields.size(); column++) {
            SqlTypeName type = fields.get(column).getType().getSqlTypeName();
            FrameSlotKind kind = kind(type);
            FrameSlot slot = describe.addFrameSlot(column, kind);
        }

        return describe;
    }

    static FrameSlotKind kind(SqlTypeName type) {
        switch (type) {
            case BOOLEAN:
                return FrameSlotKind.Boolean;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return FrameSlotKind.Long;
            case FLOAT:
            case REAL:
            case DOUBLE:
            case DECIMAL:
                return FrameSlotKind.Double;
            default:
                return FrameSlotKind.Object;
        }
    }

    static Object object(RexLiteral literal) {
        if (RexLiteral.isNullLiteral(literal))
            return SqlNull.INSTANCE;

        Object value = literal.getValue();

        switch (literal.getType().getSqlTypeName()) {
            case BOOLEAN:
                return (Boolean) value;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return ((Number) value).longValue();
            case DECIMAL:
            case FLOAT:
            case REAL:
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

    /**
     * Simplify calcite's type system into the types we actually implement at runtime.
     */
    public static RelDataType simplify(RelDataType type) {
        switch (kind(type.getSqlTypeName())) {
            case Long:
            case Int:
                return new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT);
            case Double:
            case Float:
                return new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DOUBLE);
            case Boolean:
                return new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BOOLEAN);
            case Byte:
            case Object:
            case Illegal:
            default:
                return type;
        }
    }
}
