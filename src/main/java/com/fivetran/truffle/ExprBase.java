package com.fivetran.truffle;

import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;

/**
 * Base of all SQL expressions.
 * The current row is in the FrameSlot's in VirtualFrame.
 */
abstract class ExprBase {
    /**
     * @param frame One row of data. Each FrameSlot corresponds to one column.
     * @return Result of evaluating the expression
     */
    abstract Object execute(VirtualFrame frame);

    /**
     * How the result of this expression should be represented in the VirtualFrame we create
     * and pass to the next stage of query execution
     */
    abstract FrameSlotKind kind();
}
