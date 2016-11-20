package com.fivetran.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;

/**
 * An expression that receives rows.
 *
 * Could transform rows, send them back to the user, or write them to a file somewhere.
 */
abstract class RowSink extends Node implements LateBind {
    abstract void executeVoid(VirtualFrame frame);
}
