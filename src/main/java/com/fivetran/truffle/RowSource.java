package com.fivetran.truffle;

import com.oracle.truffle.api.nodes.Node;

/**
 * Root expression that receives nothing and sends rows somewhere.
 *
 * Could be a literal, or a file somewhere.
 */
public abstract class RowSource extends Node implements LateBind {
    protected final FrameDescriptorPart sourceFrame;

    /**
     * What to do with each record
     */
    @Child
    protected RowSink then;

    protected RowSource(FrameDescriptorPart sourceFrame) {
        this.sourceFrame = sourceFrame;
    }

    protected abstract void executeVoid();

    @Override
    public void bind(LazyRowSink next) {
        if (then == null)
            then = next.apply(sourceFrame);
        else
            then.bind(next);
    }
}
