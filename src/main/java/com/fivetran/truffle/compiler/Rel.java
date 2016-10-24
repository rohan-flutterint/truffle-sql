package com.fivetran.truffle.compiler;

import com.oracle.truffle.api.nodes.*;

/**
 * Represents a relation - a physical table, a subquery, or a set of values
 */
public class Rel extends Node {
    private final Expr[] columns;

    Rel(Expr[] columns) {
        this.columns = columns;
    }

    public Expr get(int column) {
        return columns[column];
    }

    public int size() {
        return columns.length;
    }
}
