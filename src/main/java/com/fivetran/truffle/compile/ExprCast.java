package com.fivetran.truffle.compile;

import com.fivetran.truffle.Types;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameSlotKind;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.time.Instant;
import java.time.LocalDate;

/**
 * Cast any value to expected type. Unexpected types are converted to NULL.
 */
@NodeChild("target")
abstract class ExprCast extends ExprBase {
    private final RelDataType type;

    protected ExprCast(RelDataType type) {
        this.type = type;
    }

    @Specialization(guards = "asBoolean()")
    protected boolean executeBoolean(boolean value) {
        return value;
    }

    @Specialization(guards = "asLong()")
    protected long executeLong(long value) {
        return value;
    }

    @Specialization(guards = "asDouble()")
    protected double executeDouble(double value) {
        return value;
    }

    @Specialization(guards = "asLocalDate()")
    protected LocalDate executeLocalDate(LocalDate value) {
        return value;
    }

    @Specialization(guards = "asInstant()")
    protected Instant executeInstant(Instant value) {
        return value;
    }

    @Specialization(guards = "asString()")
    protected String executeString(String value) {
        return value;
    }

    @Specialization
    protected SqlNull executeNull(Object any) {
        return SqlNull.INSTANCE;
    }

    protected boolean asBoolean() {
        return Types.kind(type.getSqlTypeName()) == FrameSlotKind.Boolean;
    }

    protected boolean asLong() {
        return Types.kind(type.getSqlTypeName()) == FrameSlotKind.Long;
    }

    protected boolean asDouble() {
        return Types.kind(type.getSqlTypeName()) == FrameSlotKind.Double;
    }

    protected boolean asLocalDate() {
        return type.getSqlTypeName() == SqlTypeName.DATE;
    }

    protected boolean asInstant() {
        return type.getSqlTypeName() == SqlTypeName.TIMESTAMP;
    }

    protected boolean asString() {
        return type.getSqlTypeName() == SqlTypeName.VARCHAR;
    }
}
