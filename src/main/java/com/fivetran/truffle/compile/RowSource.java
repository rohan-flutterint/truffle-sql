package com.fivetran.truffle.compile;

import com.oracle.truffle.api.nodes.Node;

/**
 * Root expression that receives nothing and sends rows somewhere.
 *
 * Could be a literal, or a file somewhere.
 */
public abstract class RowSource extends Node implements LateBind {
    protected abstract void executeVoid();
}
