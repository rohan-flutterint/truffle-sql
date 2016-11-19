package com.fivetran.truffle;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;

import java.util.Objects;

/**
 * Compiles a RelNode into a Truffle syntax tree
 * Suppose we have a query like:
 *
 * {@code
 * SELECT id || ', ' || attr
 * FROM 's3://some-file.parquet'
 * }
 *
 * We want to emit code like:
 *
 * {@code
 * function main() {
 *     file = open('s3://some-file.parquet')
 *
 *     while (file.next())
 *       int id = file.getInt('id')
 *       Object attr = file.getAny('attr')
 *       emit(id + ', ' + attr) // .. send to user, save to s3, or whatever we want to do with the query
 * }
 * }
 */
class CompileRel implements RelShuttle {
    // TODO actually compile

    // Used to sneakily return the result to
    private RowSource compiled;

    public static RowSource compile(RelNode rel) {
        CompileRel compiler = new CompileRel();

        rel.accept(compiler);

        Objects.requireNonNull(compiler.compiled, "Compiler did not produce any output");

        return compiler.compiled;
    }

    // Force using compile
    private CompileRel() {
    }

    @Override
    public RelNode visit(TableScan scan) {
        if (scan instanceof CompileRowSource) {
            CompileRowSource compile = (CompileRowSource) scan;

            compiled = compile.compile();

            return scan;
        }
        else throw new UnsupportedOperationException("Don't know what to do with " + scan.getClass());
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        throw new UnsupportedOperationException("TableFunctionScan");
    }

    @Override
    public RelNode visit(LogicalValues values) {
        compiled = new RelLiteral(values);

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

        if (project.getInputs().isEmpty())
            compiled = new RelEmpty();
        else {
            RelNode input = project.getInputs().get(0);

            compiled = compile(input);
        }

        compiled.bind(sourceFrame -> new RelProject(sourceFrame, project));

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
        RowSource[] sources = union.getInputs()
                .stream()
                .map(i -> compile(i))
                .toArray(RowSource[]::new);

        compiled = new RelUnion(sources);

        return union;
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
