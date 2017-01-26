package com.fivetran.truffle.compile;

/**
 * An almost-compiled RowSource
 */
@FunctionalInterface
public interface ThenRowSource {
    /**
     * Compile into an executable Truffle expression
     */
    RowSource compile(ThenRowSink next);
}
