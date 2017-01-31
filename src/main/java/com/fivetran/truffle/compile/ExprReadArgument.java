package com.fivetran.truffle.compile;

import com.oracle.truffle.api.frame.VirtualFrame;

/**
 * Reads an argument from VirtualFrame
 *
 * Based on SLReadArgumentNode
 */
class ExprReadArgument extends ExprBase {
    private final int index;

    ExprReadArgument(int index) {
        this.index = index;
    }

    @Override
    Object executeGeneric(VirtualFrame frame) {
        return frame.getArguments()[index];
    }
}
