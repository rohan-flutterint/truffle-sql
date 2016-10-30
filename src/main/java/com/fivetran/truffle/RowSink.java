package com.fivetran.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

public abstract class RowSink extends Node {
    /**
     * This row transform expects to be called with its inputs arranged in sourceFrame.
     *
     * For example, if the query is SELECT x+y, x*y FROM row_source,
     * and x: int and y: double, and the expected stack frame is [int, double]
     */
    protected final FrameDescriptor sourceFrame;

    public RowSink(FrameDescriptor sourceFrame) {
        this.sourceFrame = sourceFrame;
    }

    public abstract Object execute(VirtualFrame frame);
}
