package com.fivetran.truffle;

public class RelUnion extends RowSource {
    @Children
    private final RowSource[] all;

    protected RelUnion(RowSource[] all) {
        super(FrameDescriptorPart.root(0));

        this.all = all;
    }

    @Override
    public void executeVoid() {
        // TODO these aren't linked to then
        for (RowSource each : all)
            each.executeVoid();
    }
}
