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
        // TODO it would probably be better to treat `next` as a function and invoke it
        // I think this effectively copies the `next` expression into each part of the union
        for (RowSource each : all)
            each.bind(next);
    }
}
