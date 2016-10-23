package com.fivetran.truffle;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.SourceSection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;

import java.util.function.Consumer;

/**
 * Compiles a RelNode into a Truffle syntax tree
 */
public class LocalCompiler implements RelShuttle {
    // TODO actually compile

    RootNode compiled = new RootNode(TruffleSqlLanguage.class, SourceSection.createUnavailable("Fake", "main"), new FrameDescriptor()) {
        @Override
        public Object execute(VirtualFrame frame) {
            TruffleSqlContext context = (TruffleSqlContext) frame.getArguments()[0];
            Sink sink = (Sink) frame.getArguments()[1];

            sink.accept(new Object[] { 1, "one" });
            sink.accept(new Object[] { 2, "two" });

            return null;
        }
    };

    @Override
    public RelNode visit(TableScan scan) {
        return null;
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        return null;
    }

    @Override
    public RelNode visit(LogicalValues values) {
        return null;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        return null;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        return null;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        return null;
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        return null;
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        return null;
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        return null;
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        return null;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        return null;
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        return null;
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        return null;
    }

    @Override
    public RelNode visit(RelNode other) {
        return null;
    }
}
