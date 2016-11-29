package com.fivetran.truffle.compile;

/**
 * A SQL expression that transforms 1 row of data at a time.
 *
 * For example [x+y, x*y] in SELECT x+y, x*y FROM row_source
 */
abstract class RowTransform extends RowSink {

    /**
     * What to do with each row this produces
     */
    @Child
    protected RowSink then;

    protected RowTransform(RowSink then) {
        this.then = then;
    }
}
