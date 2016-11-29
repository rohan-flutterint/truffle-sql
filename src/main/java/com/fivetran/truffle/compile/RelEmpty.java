package com.fivetran.truffle.compile;

import com.oracle.truffle.api.Truffle;

/**
 * A placeholder when the FROM clause is absent.
 * Generates 1 row with nothing in it.
 */
public class RelEmpty extends RowSourceSimple {

    public static RowSource compile(ThenRowSink next) {
        FrameDescriptorPart empty = FrameDescriptorPart.root(0);

        return new RelEmpty(empty, next.apply(empty));
    }

    private RelEmpty(FrameDescriptorPart empty, RowSink next) {
        super(empty, next);

        assert empty.size() == 0;
    }

    @Override
    public void executeVoid() {
        then.executeVoid(Truffle.getRuntime().createVirtualFrame(new Object[] { }, sourceFrame.frame()));
    }
}
