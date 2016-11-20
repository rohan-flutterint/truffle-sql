package com.fivetran.truffle.compile;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.SourceSection;

class SqlRootNode extends RootNode {
    @Child
    private RowSource delegate;

    protected SqlRootNode(SourceSection sourceSection, RowSource delegate) {
        super(TruffleSqlLanguage.class, sourceSection, new FrameDescriptor());

        this.delegate = delegate;
    }

    @Override
    public Object execute(VirtualFrame frame) {
        delegate.executeVoid();

        return null;
    }
}
