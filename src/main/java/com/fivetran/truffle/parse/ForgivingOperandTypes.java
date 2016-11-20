package com.fivetran.truffle.parse;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;

import static org.apache.calcite.sql.type.OperandTypes.*;

/**
 * Override some of the type checkers in {@link OperandTypes}
 */
class ForgivingOperandTypes {

    /**
     * Operand type-checking strategy where operand types must allow unordered
     * comparisons.
     */
    public static final SqlOperandTypeChecker COMPARABLE_UNORDERED_COMPARABLE_UNORDERED_FORGIVING =
            new ForgivingComparableOperandTypeChecker(2, RelDataTypeComparability.UNORDERED, SqlOperandTypeChecker.Consistency.LEAST_RESTRICTIVE);

    public static final SqlSingleOperandTypeChecker BOOLEAN_BOOLEAN_FORGIVING =
            family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.BOOLEAN);

    private static final SqlSingleOperandTypeChecker NUMERIC_NUMERIC_FORGIVING =
            family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC);

    public static final SqlSingleOperandTypeChecker PLUS_OPERATOR =
            OperandTypes.or(NUMERIC_NUMERIC_FORGIVING, INTERVAL_SAME_SAME, DATETIME_INTERVAL, INTERVAL_DATETIME);

    public static final SqlSingleOperandTypeChecker MINUS_OPERATOR =
            OperandTypes.or(NUMERIC_NUMERIC_FORGIVING, OperandTypes.INTERVAL_SAME_SAME, OperandTypes.DATETIME_INTERVAL);

    public static final SqlSingleOperandTypeChecker MULTIPLY_OPERATOR =
            OperandTypes.or(NUMERIC_NUMERIC_FORGIVING, INTERVAL_NUMERIC, NUMERIC_INTERVAL);

    public static final SqlSingleOperandTypeChecker DIVISION_OPERATOR =
            OperandTypes.or(NUMERIC_NUMERIC_FORGIVING, INTERVAL_NUMERIC);

    /**
     * Creates a checker that passes if each operand is a member of a
     * corresponding family.
     *
     * Forgives null values.
     */
    public static ForgivingFamilyOperandTypeChecker family(SqlTypeFamily... families) {
        return new ForgivingFamilyOperandTypeChecker(ImmutableList.copyOf(families), Predicates.<Integer>alwaysFalse());
    }
}
