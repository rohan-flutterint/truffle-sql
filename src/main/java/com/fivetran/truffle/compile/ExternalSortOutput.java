package com.fivetran.truffle.compile;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.object.DynamicObject;
import org.apache.calcite.rel.type.RelDataType;

import java.util.function.Supplier;


class ExternalSortOutput extends RowSource {
    /**
     * External sorter that produces the next row from the external sort, or null if finished.
     *
     * It's kind of weird that we have a direct reference to the external sorter.
     * The reason this works is because each Truffle program is compiled and run only once.
     */
    private final Supplier<DynamicObject> sorter;

    /**
     * Shape of frame we will allocate for this stage
     */
    private final FrameDescriptor sourceFrame;

    /**
     * Slot where we will put the result of sorter.next()
     */
    private final FrameSlot tupleSlot;

    /**
     * Write all columns of tuple from tupleSlot into VirtualFrame
     */
    @Child
    private StatementExplodeTuple explodeTuple;

    /**
     * What to do with each row from external sort
     */
    @Child
    private RowSink then;

    private ExternalSortOutput(Supplier<DynamicObject> sorter,
                               FrameDescriptor sourceFrame,
                               FrameSlot tupleSlot,
                               StatementExplodeTuple explodeTuple,
                               RowSink then) {
        this.sorter = sorter;
        this.sourceFrame = sourceFrame;
        this.tupleSlot = tupleSlot;
        this.explodeTuple = explodeTuple;
        this.then = then;
    }

    /**
     * @param sorter External sorter that will feed input to this stage
     * @param inputShape Shape of rows we created on the input side, which should indicate D
     * @param next What to do with each row in this stage
     */
    static ExternalSortOutput compile(Supplier<DynamicObject> sorter,
                                      RelDataType inputShape,
                                      ThenRowSink next) {
        // 1 slot for the DynamicObject tuple, n frames for the n exploded columns
        FrameDescriptorPart tupleFrame = FrameDescriptorPart.root(1);
        FrameDescriptorPart columnsFrame = tupleFrame.push(inputShape.getFieldCount());
        StatementExplodeTuple explodeTuple = StatementExplodeTuple.compile(tupleFrame.findFrameSlot(0), columnsFrame);
        RowSink then = next.apply(columnsFrame);

        return new ExternalSortOutput(sorter, tupleFrame.frame(), tupleFrame.findFrameSlot(0), explodeTuple, then);
    }

    @Override
    protected void executeVoid() {
        VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(new Object[]{}, sourceFrame);
        DynamicObject tuple = sorter.get();

        while (tuple != null) {
            // tuple = sorter.get()
            frame.setObject(tupleSlot, tuple);

            // a = tuple[0]
            // b = tuple[1]
            // ...
            explodeTuple.executeVoid(frame);

            // ...execute rest of stage...
            then.executeVoid(frame);

            tuple = sorter.get();
        }
    }
}