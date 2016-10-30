package com.fivetran.truffle;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotTypeException;
import com.oracle.truffle.api.frame.VirtualFrame;

public class ExprProject extends ExprBase {
    private final FrameSlot slot;

    ExprProject(FrameSlot slot) {
        this.slot = slot;
    }

    @Override
    public Object executeGeneric(VirtualFrame frame) {
        try {
            return frame.getObject(slot);
        } catch (FrameSlotTypeException e) {
            throw new RuntimeException(e);
        }
    }

}
