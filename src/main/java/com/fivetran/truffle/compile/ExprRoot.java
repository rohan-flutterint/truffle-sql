package com.fivetran.truffle.compile;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.SourceSection;

/**
 * An expression that can be converted to a {@link java.util.Comparator<com.oracle.truffle.api.object.DynamicObject>}
 * using {@link com.oracle.truffle.api.interop.java.JavaInterop#asJavaFunction(Class, TruffleObject)}
 */
class ExprRoot extends RootNode {
    @Child
    private ExprBase delegate;

    protected ExprRoot(SourceSection sourceSection, ExprBase delegate) {
        super(TruffleSqlLanguage.class, sourceSection, new FrameDescriptor());

        this.delegate = delegate;
    }

    @Override
    public Object execute(VirtualFrame frame) {
        return delegate.executeGeneric(frame);
    }
}
