package com.fivetran.truffle;

@FunctionalInterface
public interface Sink {
    void accept(Object[] row);
}
