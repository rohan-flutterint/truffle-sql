package com.fivetran.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;

/**
 * Represents a contiguous subset of frame slots that we use to store a relation
 */
class FrameDescriptorPart {
    private final FrameDescriptor frame;
    private final int startOffset;
    private final int size;

    private FrameDescriptorPart(FrameDescriptor frame, int startOffset, int size) {
        this.frame = frame;
        this.startOffset = startOffset;
        this.size = size;
    }

    /**
     * The underyling frame, which includes all the slots
     */
    public FrameDescriptor frame() {
        return frame;
    }

    /**
     * The start of the part of the frame we are using
     */
    public int startOffset() {
        return startOffset;
    }

    /**
     * The number of slots we are using
     */
    public int size() {
        return size;
    }

    public static FrameDescriptorPart root(int slots) {
        FrameDescriptor frame = new FrameDescriptor();

        for (int i = 0; i < slots; i++)
            frame.addFrameSlot(i);

        return new FrameDescriptorPart(frame, 0, slots);
    }

    public FrameDescriptorPart push(int slots) {
        for (int i = startOffset + size; i < startOffset + size + slots; i++)
            frame.addFrameSlot(i);

        return new FrameDescriptorPart(frame, startOffset + size, slots);
    }

    public FrameSlot findFrameSlot(int index) {
        return frame().findFrameSlot(startOffset + index);
    }
}
