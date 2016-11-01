package com.fivetran.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import org.apache.calcite.rel.type.RelDataType;

import java.lang.reflect.Field;
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
                    Object value = fields[column].get(row);

                    frame.setObject(slot, value);
                }

                then.executeVoid(frame);
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
