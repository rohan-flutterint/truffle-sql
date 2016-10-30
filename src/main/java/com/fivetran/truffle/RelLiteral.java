package com.fivetran.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.source.SourceSection;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

class RelLiteral extends RowSource {
    private final LogicalValues values;
    private final FrameDescriptor resultType;

    public RelLiteral(SourceSection source, LogicalValues values, RowSink then) {
        super(source, then);

        this.values = values;
        this.resultType = Types.frame(values.getRowType());
    }

    @Override
    public Object execute(VirtualFrame frame) {
        assert frame.getFrameDescriptor().getSize() == 0 : "Input to literal should be empty but was " + frame.getFrameDescriptor();

        VirtualFrame thenFrame = Truffle.getRuntime().createVirtualFrame(new Object[]{}, resultType);
        List<? extends FrameSlot> slots = resultType.getSlots();

        for (List<RexLiteral> literals : values.getTuples()) {
            for (int i = 0; i < literals.size(); i++) {
                RexLiteral literal = literals.get(i);
                Object value = Types.object(literal);
                FrameSlot slot = slots.get(i);

                thenFrame.setObject(slot, value);
            }

            then.execute(thenFrame);
        }

        return QueryReturn.INSTANCE;
    }
}
