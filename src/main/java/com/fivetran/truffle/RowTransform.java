package com.fivetran.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.source.SourceSection;

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

    protected RowTransform(SourceSection source, FrameDescriptor sourceFrame, RowSink then) {
        super(sourceFrame);

        this.then = then;
    }
}
