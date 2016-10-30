package com.fivetran.truffle;

public class RelUnion extends RowSource {
    @Children
    private final RowSource[] all;

    protected RelUnion(RowSource[] all) {
        this.all = all;
    }

    @Override
    public void executeVoid() {
        for (RowSource each : all)
            each.executeVoid();
    }
}
