package com.fivetran.truffle;

import com.oracle.truffle.api.nodes.*;

/**
 * Represents a relation - a physical table, a subquery, or a set of values
 */
public class XRel extends Node {
    private final XIterator[] columns;

    XRel(XIterator[] columns) {
        this.columns = columns;
    }

    public XIterator get(int column) {
        return columns[column];
    }

    public int size() {
        return columns.length;
    }
}
