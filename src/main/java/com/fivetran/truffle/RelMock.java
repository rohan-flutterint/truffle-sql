package com.fivetran.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.object.Shape;
import org.apache.calcite.rel.type.RelDataType;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

public class RelMock extends RowSource {
    private final RelDataType relType;
    private final Class<?> type;
    private final Object[] rows;

    @Child
    private RowSink then;

    public RelMock(RelDataType relType, Class<?> type, Object[] rows, RowSink then) {
        this.relType = relType;
        this.type = type;
        this.rows = rows;
        this.then = then;
    }

    @Override
    protected void executeVoid() {
        try {
            FrameDescriptor frameType = Types.frame(relType);
            VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(new Object[]{}, frameType);
            Field[] fields = type.getFields();
            List<? extends FrameSlot> slots = frameType.getSlots();

            for (Object row : rows) {
                for (int column = 0; column < fields.length; column++) {
                    FrameSlot slot = slots.get(column);
                    Object rawValue = fields[column].get(row);
                    RelDataType type = relType.getFieldList().get(column).getType();
                    Object truffleValue = coerce(rawValue, type);

                    frame.setObject(slot, truffleValue);
                }

                then.executeVoid(frame);
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private Object coerce(Object value, RelDataType type) {
        if (value == null)
            return SqlNull.INSTANCE;

        switch (type.getSqlTypeName()) {
            case ROW:
                return truffleObject(value, type);
            default:
                return Types.coerceAny(value, type);
        }
    }

    /**
     * Convert a Java object to a TruffleObject using reflection.
     * This is very slow! This should only be used for mocks.
     */
    private TruffleObject truffleObject(Object value, RelDataType type) {
        Shape shape = TruffleSqlContext.LAYOUT.createShape(SqlObjectType.INSTANCE);
        Class<?> clazz = value.getClass();
        Field[] fields = clazz.getFields();
        Object[] fieldValues = new Object[fields.length];

        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            RelDataType fieldType = type.getFieldList().get(i).getType();

            assert !Modifier.isStatic(field.getModifiers()) : "Mock records are not allowed to have static fields";

            try {
                Object rawValue = field.get(value);
                Object niceValue = coerce(rawValue, fieldType);

                shape = shape.defineProperty(field.getName(), niceValue, 0);
                fieldValues[i] = niceValue;
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        return shape.createFactory().newInstance(fieldValues);
    }
}
