package com.fivetran.truffle;

/**
 * A SQL expression that transforms 1 row of data at a time.
 *
 * For example [x+y, x*y] in SELECT x+y, x*y FROM row_source
 */
public abstract class RowTransform extends RowSink {

    /**
     * What to do with each row this produces
     */
    @Child
    protected RowSink then;

    protected RowTransform() {
    }

    @Override
    public void bind(LazyRowSink next) {
        if (then == null)
            then = next.apply(frame());
        else
            then.bind(next);
    }

    public abstract FrameDescriptorPart frame();
}
