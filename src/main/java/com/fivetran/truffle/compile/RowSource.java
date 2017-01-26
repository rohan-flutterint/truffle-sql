package com.fivetran.truffle.compile;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

/**
 * Root expression that receives nothing and sends rows somewhere.
 *
 * Could be a literal, or a file somewhere.
 */
public abstract class RowSource extends Node {
    /**
     * Flush all rows. Called once for the entire execution of the query.
     */
    protected abstract void executeVoid(VirtualFrame frame);
}
