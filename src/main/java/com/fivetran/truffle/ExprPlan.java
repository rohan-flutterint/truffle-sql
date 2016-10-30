package com.fivetran.truffle;

import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.RootNode;
import org.apache.calcite.rel.RelRoot;

/**
 * By the time we send our query to Truffle, it has already been parsed, validated, and planned.
 * This pseudo-node simply holds the query plan so we can pass it as the "context" parameter to {@link TruffleSqlLanguage#parse}
 */
public class ExprPlan extends Node {
    public final RelRoot plan;
    public final RootNode then;

    public ExprPlan(RelRoot plan, RootNode then) {
        this.plan = plan;
        this.then = then;
    }
}
