package com.fivetran.truffle;

import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleRuntime;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.DirectCallNode;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.SourceSection;

/**
 * Root expression that receives nothing and sends rows somewhere.
 * Includes literals, tables.
 */
abstract class RowSource extends RootNode {
    /**
     * What to do with each row this RowSource produces.
     */
    final RootNode then;

    protected RowSource(SourceSection source, RootNode then) {
        super(TruffleSqlLanguage.class, source, null);

        this.then = then;
    }
}
