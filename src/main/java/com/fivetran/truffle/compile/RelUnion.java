package com.fivetran.truffle.compile;

public class RelUnion extends RowSource {
    @Children
    private final RowSource[] all;

    public RelUnion(RowSource[] all) {
        this.all = all;
    }

    @Override
    public void executeVoid() {
        for (RowSource each : all)
            each.executeVoid();
    }

    @Override
    public void bind(LazyRowSink next) {
        for (RowSource each : all)
            each.bind(next);
    }
}
