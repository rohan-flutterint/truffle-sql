package com.fivetran.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;

/**
 * A placeholder when the FROM clause is absent.
 * Generates 1 row with nothing in it.
 */
public class RelEmpty extends RowSource {

    private final FrameDescriptor resultType;

    @Child
    private RowTransform then;

    public RelEmpty(RowTransform then) {
        this.resultType = new FrameDescriptor();
        this.then = then;
    }

    @Override
    public void executeVoid() {
        then.executeVoid(Truffle.getRuntime().createVirtualFrame(new Object[] { }, resultType));
    }
}
