package com.fivetran.truffle;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Convert logical plan (for example LogicalProject) to physical plan (for example TProject)
 */
public class CompileLogical implements RelShuttle {

    public static TRel compile(RelNode logical) {
        return (TRel) logical.accept(new CompileLogical());
    }

    private CompileLogical() { }

    @Override
    public RelNode visit(TableScan scan) {
        if (scan instanceof TRel)
            return scan;
        else
            throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalValues values) {
        return new TValues(
                values.getCluster(),
                values.getRowType(),
                values.getTuples(),
                values.getTraitSet().replace(TRel.CONVENTION)
        );
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalProject project) {
        RelNode input = compile(project.getInput());

        return new TProject(project.getCluster(),
                            project.getTraitSet().replace(TRel.CONVENTION),
                            input,
                            project.getProjects(),
                            project.getRowType()
        );
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
        List<RelNode> inputs = union.getInputs()
                .stream()
                .map(CompileLogical::compile)
                .collect(Collectors.toList());

        return new TUnion(
                union.getCluster(),
                union.getTraitSet().replace(TRel.CONVENTION),
                inputs,
                union.all
        );
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
        if (other instanceof TRel)
            return other;
        else
            throw new UnsupportedOperationException("Don't know how to compile " + other);
    }
}
