package com.fivetran.truffle.compile;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.SourceSection;

/**
 * Wraps a RowSource so we can call Truffle.getRuntime().createCallTarget(RootNode)
 */
class RelRootNode extends RootNode {
    @Child
    private RowSource delegate;

    protected RelRootNode(SourceSection sourceSection, RowSource delegate) {
        super(TruffleSqlLanguage.class, sourceSection, new FrameDescriptor());

        this.delegate = delegate;
    }

    @Override
    public Object execute(VirtualFrame frame) {
        delegate.executeVoid();

        return null;
    }
}
