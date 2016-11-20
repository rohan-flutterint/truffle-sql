package com.fivetran.truffle.parse;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

/**
 * Override some of the operators in {@link org.apache.calcite.sql.fun.SqlStdOperatorTable}
 */
public class ForgivingOperatorTable extends ReflectiveSqlOperatorTable {

    /**
     * Logical equals operator, '<code>=</code>'.
     */
    public static final SqlBinaryOperator EQUALS =
            new SqlBinaryOperator(
                    "=",
                    SqlKind.EQUALS,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    ForgivingOperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED_FORGIVING);

    /**
     * Logical not-equals operator, '<code>&lt;&gt;</code>'.
     */
    public static final SqlBinaryOperator NOT_EQUALS =
            new SqlBinaryOperator(
                    "<>",
                    SqlKind.NOT_EQUALS,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    ForgivingOperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED_FORGIVING);

    /**
     * Logical <code>AND</code> operator.
     */
    public static final SqlBinaryOperator AND =
            new SqlBinaryOperator(
                    "AND",
                    SqlKind.AND,
                    24,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED,
                    InferTypes.BOOLEAN,
                    ForgivingOperandTypes.BOOLEAN_BOOLEAN_FORGIVING);

    /**
     * Logical <code>OR</code> operator.
     */
    public static final SqlBinaryOperator OR =
            new SqlBinaryOperator(
                    "OR",
                    SqlKind.OR,
                    22,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED,
                    InferTypes.BOOLEAN,
                    ForgivingOperandTypes.BOOLEAN_BOOLEAN_FORGIVING);

    /**
     * Infix arithmetic plus operator, '<code>+</code>'.
     */
    public static final SqlBinaryOperator PLUS =
            new SqlMonotonicBinaryOperator(
                    "+",
                    SqlKind.PLUS,
                    40,
                    true,
                    ReturnTypes.NULLABLE_SUM,
                    InferTypes.FIRST_KNOWN,
                    ForgivingOperandTypes.PLUS_OPERATOR);

    public static final SqlBinaryOperator MINUS =
            new SqlMonotonicBinaryOperator(
                    "-",
                    SqlKind.MINUS,
                    40,
                    true,
                    ReturnTypes.NULLABLE_SUM,
                    InferTypes.FIRST_KNOWN,
                    ForgivingOperandTypes.MINUS_OPERATOR
            );

    /**
     * Arithmetic multiplication operator, '<code>*</code>'.
     */
    public static final SqlBinaryOperator MULTIPLY =
            new SqlMonotonicBinaryOperator(
                    "*",
                    SqlKind.TIMES,
                    60,
                    true,
                    ReturnTypes.PRODUCT_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    ForgivingOperandTypes.MULTIPLY_OPERATOR);

    /**
     * Arithmetic division operator, '<code>/</code>'.
     */
    public static final SqlBinaryOperator DIVIDE =
            new SqlBinaryOperator(
                    "/",
                    SqlKind.DIVIDE,
                    60,
                    true,
                    ReturnTypes.QUOTIENT_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    ForgivingOperandTypes.DIVISION_OPERATOR);

    private static ForgivingOperatorTable instance;

    public static ForgivingOperatorTable instance() {
        if (instance == null) {
            instance = new ForgivingOperatorTable();

            instance.init();
        }

        return instance;
    }
}
