package com.fivetran.truffle.compiler;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Objects;

/**
 * Compiles a RelNode into a Truffle syntax tree
 */
public class CompileRel implements RelShuttle {
    // TODO actually compile

    // Used to sneakily return the result to
    private Rel compiled;

    public static Rel compile(RelNode rel) {
        CompileRel compiler = new CompileRel();

        rel.accept(compiler);

        Objects.requireNonNull(compiler.compiled, "Compiler did not produce any output");

        return compiler.compiled;
    }

    // Force using compile
    private CompileRel() { }

    @Override
    public RelNode visit(TableScan scan) {
        throw new UnsupportedOperationException("TableScan should be wrapped in LogicalProject");
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        throw new UnsupportedOperationException("TableFunctionScan");
    }

    @Override
    public RelNode visit(LogicalValues values) {
        Expr[] columns = new Expr[values.getRowType().getFieldCount()];

        for (int column = 0; column < columns.length; column++)
            columns[column] = new LiteralIterator(values, column);

        compiled = new Rel(columns);

        return values;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalProject project) {
        // Multiple inputs should never occur in LogicalProject, only in other RelNode implementers like MultiJoin
        assert project.getInputs().size() <= 1 : "LogicalProject has " + project.getInputs().size() + " inputs";

        List<RexNode> childExps = project.getChildExps();
        Rel input = project.getInputs().isEmpty() ? null : compile(project.getInput(0));

        Expr[] columns = childExps.stream()
                .map(child -> child.accept(new CompileExpr(input)))
                .toArray(Expr[]::new);

        compiled = new Rel(columns);

        return project;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(RelNode other) {
        throw new UnsupportedOperationException();
    }
}
