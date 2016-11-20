package com.fivetran.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

/**
 * A VALUES literal
 */
public class RelLiteral extends RowSourceSimple {
    private final Values values;

    public RelLiteral(Values values) {
        super(FrameDescriptorPart.root(values.getRowType().getFieldCount()));

        this.values = values;
    }

    @Override
    public void executeVoid() {
        VirtualFrame thenFrame = Truffle.getRuntime().createVirtualFrame(new Object[]{}, sourceFrame.frame());
        List<? extends FrameSlot> slots = sourceFrame.frame().getSlots();

        for (List<RexLiteral> literals : values.getTuples()) {
            for (int i = 0; i < literals.size(); i++) {
                RexLiteral literal = literals.get(i);
                Object value = Types.coerceLiteral(literal);
                FrameSlot slot = slots.get(i);

                RelMock.setSlot(thenFrame, slot, value);
            }

            then.executeVoid(thenFrame);
        }
    }
}
