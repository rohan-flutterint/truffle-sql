package com.fivetran.truffle;

import com.oracle.truffle.api.Truffle;

/**
 * A placeholder when the FROM clause is absent.
 * Generates 1 row with nothing in it.
 */
public class RelEmpty extends RowSourceSimple {

    public RelEmpty() {
        super(FrameDescriptorPart.root(0));
    }

    @Override
    public void executeVoid() {
        then.executeVoid(Truffle.getRuntime().createVirtualFrame(new Object[] { }, sourceFrame.frame()));
    }
}
