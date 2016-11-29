package com.fivetran.truffle.compile;

import com.fivetran.truffle.Projection;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Reads a primitive field.
 */
@NodeChild(value = "columnReader", type = ExprReadLocal.class)
abstract class ExprReadColumn extends ExprBase {
    private final PrimitiveType type;
    private final int maxDefinitionLevel;

    protected ExprReadColumn(MessageType root, Projection path) {
        this.type = (PrimitiveType) root.getType(path.path);
        this.maxDefinitionLevel = root.getMaxDefinitionLevel(path.path);

        assert root.getMaxRepetitionLevel(path.path) == 0 : "Repeated fields should be flattened using table expressions";
    }

    /**
     * If column is not nullable, we can just get its primitive value without checking the definition level.
     */
    @Specialization(guards = {"!isNullable()", "isBoolean()"})
    protected boolean getBoolean(ColumnReader currentFile) {
        return doGetBoolean(currentFile);
    }

    protected boolean isBoolean() {
        return type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BOOLEAN;
    }

    /**
     * If column is nullable, optimistically assume it is never null and try to get a primitive value.
     * If definitionLevel < maxDefinitionLevel, abandon this path.
     */
    @Specialization(guards = "isBoolean()", rewriteOn = NotDefinedException.class)
    protected boolean tryGetBoolean(ColumnReader currentFile) {
        if (isNull(currentFile))
            throw new NotDefinedException();
        else
            return doGetBoolean(currentFile);
    }

    @TruffleBoundary
    private static boolean doGetBoolean(ColumnReader reader) {
        boolean result = reader.getBoolean();

        reader.consume();

        return result;
    }

    /**
     * Slow path, returns boxed values.
     */
    @Specialization(guards = {"isNullable()", "isBoolean()"})
    protected Object getNullableBoolean(ColumnReader currentFile) {
        if (isNull(currentFile))
            return doGetNull(currentFile);
        else
            return doGetBoolean(currentFile);
    }

    /**
     * If column is not nullable, we can just get its primitive value without checking the definition level.
     */
    @Specialization(guards = {"!isNullable()", "isDouble()"})
    protected double getDouble(ColumnReader currentFile) {
        return doGetDouble(currentFile);
    }

    protected boolean isDouble() {
        switch (type.getPrimitiveTypeName()) {
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    /**
     * If column is nullable, optimistically assume it is never null and try to get a primitive value.
     * If definitionLevel < maxDefinitionLevel, abandon this path.
     */
    @Specialization(guards = "isDouble()", rewriteOn = NotDefinedException.class)
    protected double tryGetDouble(ColumnReader currentFile) {
        if (isNull(currentFile))
            throw new NotDefinedException();
        else
            return doGetDouble(currentFile);
    }

    @TruffleBoundary
    private static double doGetDouble(ColumnReader reader) {
        double result = reader.getDouble();

        reader.consume();

        return result;
    }

    /**
     * Slow path, returns boxed values.
     */
    @Specialization(guards = {"isNullable()", "isDouble()"})
    protected Object getNullableDouble(ColumnReader currentFile) {
        if (isNull(currentFile))
            return doGetNull(currentFile);
        else
            return doGetDouble(currentFile);
    }

    /**
     * If column is not nullable, we can just get its primitive value without checking the definition level.
     */
    @Specialization(guards = {"!isNullable()", "isLong()"})
    protected long getLong(ColumnReader currentFile) {
        return doGetLong(currentFile);
    }

    @TruffleBoundary
    private static long doGetLong(ColumnReader reader) {
        long result = reader.getLong();

        reader.consume();

        return result;
    }

    protected boolean isLong() {
        switch (type.getPrimitiveTypeName()) {
            case INT32:
            case INT64:
                return true;
            default:
                return false;
        }
    }

    /**
     * If column is nullable, optimistically assume it is never null and try to get a primitive value.
     * If definitionLevel < maxDefinitionLevel, abandon this path.
     */
    @Specialization(guards = "isLong()", rewriteOn = NotDefinedException.class)
    protected long tryGetLong(ColumnReader currentFile) {
        if (isNull(currentFile))
            throw new NotDefinedException();
        else
            return doGetLong(currentFile);
    }

    /**
     * Slow path, returns boxed values.
     */
    @Specialization(guards = {"isNullable()", "isLong()"})
    protected Object getNullableLong(ColumnReader currentFile) {
        if (isNull(currentFile))
            return doGetNull(currentFile);
        else
            return doGetLong(currentFile);
    }

    /**
     * Slow path, returns boxed values.
     */
    @Specialization(guards = {"isString()"})
    protected Object getNullableString(ColumnReader currentFile) {
        if (isNull(currentFile))
            return doGetNull(currentFile);
        else
            return doGetString(currentFile);
    }

    @TruffleBoundary
    private static String doGetString(ColumnReader reader) {
        String result = reader.getBinary().toStringUsingUTF8();

        reader.consume();

        return result;
    }

    @TruffleBoundary
    private static SqlNull doGetNull(ColumnReader reader) {
        reader.consume();

        return SqlNull.INSTANCE;
    }

    protected boolean isString() {
        return type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY;
    }

    @TruffleBoundary
    protected boolean isNull(ColumnReader reader) {
        return reader.getCurrentDefinitionLevel() < maxDefinitionLevel;
    }

    protected boolean isNullable() {
        return type.getRepetition() == Type.Repetition.OPTIONAL;
    }
}
