package com.fivetran.truffle;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.util.Calendar;

class XLiteral extends XIterator {
    private final LogicalValues values;
    private final int column;
    private int row;

    public XLiteral(LogicalValues values, int column) {
        this.values = values;
        this.column = column;
    }

    @Override
    public long getTotalValueCount() {
        return values.getTuples().size();
    }

    @Override
    public int getCurrentRepetitionLevel() {
        return 0;
    }

    @Override
    public int getCurrentDefinitionLevel() {
        return 0;
    }

    @Override
    public boolean isNull() {
        return RexLiteral.isNullLiteral(pop());
    }

    @Override
    public CharSequence getString() {
        return RexLiteral.stringValue(pop());
    }

    @Override
    public boolean isString() {
        return isA(String.class);
    }

    @Override
    public int getInteger() {
        return RexLiteral.intValue(pop());
    }

    @Override
    public boolean isInteger() {
        return isA(Integer.class) || isA(int.class);
    }

    @Override
    public boolean getBoolean() {
        return RexLiteral.booleanValue(pop());
    }

    @Override
    public boolean isBoolean() {
        return isA(Boolean.class) || isA(boolean.class);
    }

    @Override
    public long getLong() {
        return getAs(BigDecimal.class).longValue();
    }

    @Override
    public boolean isLong() {
        return isA(Long.class) || isA(long.class);
    }

    @Override
    public float getFloat() {
        return getAs(Float.class);
    }

    @Override
    public boolean isFloat() {
        return isA(Float.class) || isA(float.class);
    }

    @Override
    public double getDouble() {
        return getAs(Double.class);
    }

    @Override
    public boolean isDouble() {
        return isA(Double.class) || isA(double.class);
    }

    @Override
    public <T> int getDict(Class<T> type) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public <T> boolean isDict(Class<T> type) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public void skip() {
        row++;
    }

    @Override
    public boolean isFullyConsumed() {
        return row == getTotalValueCount();
    }

    /**
     * This might be problematic if it becomes inconsistent with {@link com.fivetran.truffle.TruffleMeta#typeFactory}
     */
    private static final JavaTypeFactoryImpl TYPES = new JavaTypeFactoryImpl();

    <T> boolean isA(Class<T> type) {
        Class<?> javaClass = (Class<?>) TYPES.getJavaClass(peek().getType());

        return type.isAssignableFrom(javaClass);
    }

    <T> T getAs(Class<T> type) {
        Object value = getObject();

        if (!type.isInstance(value))
            throw new WrongTypeException();
        else {
            row++;

            return type.cast(value);
        }
    }

    @Override
    public Object getObject() {
        RexLiteral literal = pop();
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

    private RexLiteral pop() {
        RexLiteral result = peek();

        row++;

        return result;
    }

    private RexLiteral peek() {
        return values.getTuples().get(row).get(column);
    }
}
