package com.fivetran.truffle.compile;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.object.DynamicObject;

/**
 * Materializes an entire FrameDescriptorPart as a DynamicObject
 */
class ExprMaterializeTuple extends ExprBase {

    private final FrameSlot tupleSlot;

    /**
     * Write each column to a DynamicObject
     */
    @Children
    private final StatementWriteProperty[] writeColumns;

    @Child
    private ExprReadLocal readTuple;

    private ExprMaterializeTuple(FrameSlot tupleSlot,
                                 StatementWriteProperty[] writeColumns,
                                 ExprReadLocal readTuple) {
        this.tupleSlot = tupleSlot;
        this.readTuple = readTuple;
        this.writeColumns = writeColumns;
    }

    /**
     * Compile a tuple expression like (a, b, c) to a program like:
     *
     * tuple = new()
     * tuple[0] = a
     * tuple[1] = b
     * tuple[2] = c
     * tuple
     */
    static ExprMaterializeTuple compile(FrameDescriptorPart from) {
        // We need one more frame slot to hold the tuple as we materialize it
        FrameDescriptorPart tupleFrame = from.push(1);
        FrameSlot tupleSlot = tupleFrame.findFrameSlot(0);

        // tuple[?] = ?
        ExprReadLocal readTuple = ExprReadLocalNodeGen.create(tupleSlot);
        StatementWriteProperty[] writeColumns = new StatementWriteProperty[from.size()];

        for (int index = 0; index < from.size(); index++) {
            FrameSlot slot = from.findFrameSlot(index);
            ExprReadLocal readColumn = ExprReadLocalNodeGen.create(slot);
            writeColumns[index] = StatementWritePropertyNodeGen.create(
                    Integer.toString(index),
                    readTuple,
                    readColumn
            );
        }

        return new ExprMaterializeTuple(tupleSlot, writeColumns, readTuple);
    }

    @Override
    @ExplodeLoop
    Object executeGeneric(VirtualFrame frame) {
        // Create a new tuple and store it in frame
        // TODO it might be better to cache the Shape here, rather than in each StatementWriteLocal
        // Right now each tuple starts as a blank-slate DynamicObject and re-acquires types based on the observed value
        // Since types should be consistent from row to row, consider caching the Shape once in ExprMaterializeLocal
        // and allocating DynamicObject with predefined Shape
        DynamicObject tuple = TruffleSqlContext.EMPTY.newInstance();

        frame.setObject(tupleSlot, tuple);

        // Write each property of the tuple
        for (StatementWriteProperty each : writeColumns) {
            each.executeVoid(frame);
        }

        return readTuple.executeGeneric(frame);
    }
}
