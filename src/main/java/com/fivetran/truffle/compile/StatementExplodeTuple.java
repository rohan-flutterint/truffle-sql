package com.fivetran.truffle.compile;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;

/**
 * Explodes a DynamicObject into VirtualFrame
 */
class StatementExplodeTuple extends StatementBase {

    @Children
    private final StatementWriteLocal[] writeColumns;

    private StatementExplodeTuple(StatementWriteLocal[] writeColumns) {
        this.writeColumns = writeColumns;
    }

    static StatementExplodeTuple compile(FrameSlot tupleFrame, FrameDescriptorPart columnsFrame) {
        ExprReadLocal readTuple = ExprReadLocalNodeGen.create(tupleFrame);
        StatementWriteLocal[] writeColumns = new StatementWriteLocal[columnsFrame.size()];

        for (int index = 0; index < columnsFrame.size(); index++) {
            ExprReadProperty readProperty = ExprReadPropertyNodeGen.create(Integer.toString(index), readTuple);
            FrameSlot slot = columnsFrame.findFrameSlot(index);
            StatementWriteLocal writeColumn = StatementWriteLocalNodeGen.create(readProperty, slot);

            writeColumns[index] = writeColumn;
        }

        return new StatementExplodeTuple(writeColumns);
    }

    @Override
    @ExplodeLoop
    void executeVoid(VirtualFrame frame) {
        for (StatementWriteLocal each : writeColumns) {
            each.executeVoid(frame);
        }
    }
}
