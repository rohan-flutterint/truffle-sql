package com.fivetran.truffle;

import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.SourceSection;

/**
 * Root expression that receives nothing and sends rows somewhere.
 *
 * Could be a literal, or a file somewhere.
 */
public abstract class RowSource extends RootNode {
    /**
     * What to do with each row this RowSource produces.
     */
    @Child
    protected RowSink then;

    protected RowSource(SourceSection source, RowSink then) {
        super(TruffleSqlLanguage.class, source, null);

        this.then = then;
    }
}
