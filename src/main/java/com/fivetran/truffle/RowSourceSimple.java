package com.fivetran.truffle;

/**
 * Most RowSources send rows from 1 place, for example a file, to 1 place, for example a LogicalProject
 */
public abstract class RowSourceSimple extends RowSource {
    protected final FrameDescriptorPart sourceFrame;

    /**
     * What to do with each record
     */
    @Child
    protected RowSink then;

    protected RowSourceSimple(FrameDescriptorPart sourceFrame) {
        this.sourceFrame = sourceFrame;
    }

    @Override
    public void bind(LazyRowSink next) {
        if (then == null)
            then = next.apply(sourceFrame);
        else
            then.bind(next);
    }
}
