package com.fivetran.truffle;

import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.nodes.DirectCallNode;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.SourceSection;

/**
 * A SQL expression that transforms 1 row of data.
 *
 * For example [x+y, x*y] in SELECT x+y, x*y FROM row_source
 */
abstract class RowTransform extends RootNode {
    /**
     * What to do with each row this produces
     */
    final RootNode then;

    protected RowTransform(SourceSection source, FrameDescriptor sourceFrame, RootNode then) {
        super(TruffleSqlLanguage.class, source, sourceFrame);

        this.then = then;
    }

    /**
     * Describes the rows this row transform sends to then.
     * Analogous to getFrameDescriptor(), which describes the rows this transform receives.
     *
     * For example, the transform [x+y, x*y, x/y] might have
     *
     * getFrameDescriptor() = [int, double]
     *
     * and
     *
     * getResultFrameDescriptor() = [double, double, double]
     *
     * if x is int and y is double.
     */
    abstract FrameDescriptor getResultFrameDescriptor();
}
