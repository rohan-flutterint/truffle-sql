package com.fivetran.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

/**
 * A VALUES literal
 */
public class RelLiteral extends RowSource {
    private final LogicalValues values;
    private final FrameDescriptor resultType;

    @Child
    private RowSink then;

    public RelLiteral(LogicalValues values, RowSink then) {
        this.values = values;
        this.resultType = Types.frame(values.getRowType());
        this.then = then;
    }

    @Override
    public void executeVoid() {
        VirtualFrame thenFrame = Truffle.getRuntime().createVirtualFrame(new Object[]{}, resultType);
        List<? extends FrameSlot> slots = resultType.getSlots();

        for (List<RexLiteral> literals : values.getTuples()) {
            for (int i = 0; i < literals.size(); i++) {
                RexLiteral literal = literals.get(i);
                Object value = Types.coerceLiteral(literal);
                FrameSlot slot = slots.get(i);

                thenFrame.setObject(slot, value);
            }

            then.executeVoid(thenFrame);
        }
    }
}
