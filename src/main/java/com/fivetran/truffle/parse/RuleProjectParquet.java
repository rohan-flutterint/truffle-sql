package com.fivetran.truffle.parse;

import com.fivetran.truffle.NamedProjection;
import com.fivetran.truffle.Projection;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.*;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class RuleProjectParquet extends RelOptRule {
    static final RuleProjectParquet INSTANCE = new RuleProjectParquet();

    private RuleProjectParquet() {
        super(operand(PhysicalProject.class, operand(PhysicalParquet.class, none())), RuleProjectParquet.class.getSimpleName());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhysicalProject project = call.rel(0);
        PhysicalParquet scan = call.rel(1);
        List<String> fieldNames = scan.project.stream().map(f -> f.name).collect(Collectors.toList());

        // When the query contains an expression like SELECT a.b+1 FROM tbl, onMatch gets called multiple times
        //
        // The first time it has just the references, for example SELECT a FROM tbl
        // We push the references down into ParquetTableScan
        //
        // Then it gets called with nested references, for example SELECT a.b FROM tbl
        // Again we are able to push down the references into ParquetTableScan
        //
        // The last time it has the expressions, for example SELECT a.b+1 FROM tbl
        // There's nothing more we can do with expressions, so we do nothing
        try {
            List<NamedProjection> projections = paths(fieldNames, project);
            PhysicalParquet pushdown = scan.withProject(projections);

            call.transformTo(pushdown);
        } catch (ComplexExpressionException e) {
            // Nothing to do
        }
    }

    /**
     * When we encounter a complex expression like table.column + 1, we can't push the expression down into PhysicalParquet.
     */
    private static class ComplexExpressionException extends RuntimeException {
    }

    private static List<NamedProjection> paths(List<String> fieldNames, PhysicalProject project) {
        List<NamedProjection> acc = new ArrayList<>();

        for (Pair<RexNode, String> each : project.getNamedProjects()) {
            RexNode exp = each.getKey();
            String name = each.getValue();
            Projection projection = path(fieldNames, exp);

            acc.add(new NamedProjection(name, projection));
        }

        return acc;
    }

    /**
     * If exp looks like a.b.c, return Projection.of(a, b, c);
     * otherwise return Projection.of()
     */
    private static Projection path(List<String> fieldNames, RexNode exp) {
        return exp.accept(new RexVisitor<Projection>() {
            @Override
            public Projection visitInputRef(RexInputRef inputRef) {
                String head = fieldNames.get(inputRef.getIndex());

                return Projection.of(head);
            }

            @Override
            public Projection visitLocalRef(RexLocalRef localRef) {
                throw new ComplexExpressionException();
            }

            @Override
            public Projection visitLiteral(RexLiteral literal) {
                throw new ComplexExpressionException();
            }

            @Override
            public Projection visitCall(RexCall call) {
                throw new ComplexExpressionException();
            }

            @Override
            public Projection visitOver(RexOver over) {
                throw new ComplexExpressionException();
            }

            @Override
            public Projection visitCorrelVariable(RexCorrelVariable correlVariable) {
                throw new ComplexExpressionException();
            }

            @Override
            public Projection visitDynamicParam(RexDynamicParam dynamicParam) {
                throw new ComplexExpressionException();
            }

            @Override
            public Projection visitRangeRef(RexRangeRef rangeRef) {
                throw new ComplexExpressionException();
            }

            @Override
            public Projection visitFieldAccess(RexFieldAccess fieldAccess) {
                RexNode reference = fieldAccess.getReferenceExpr();
                Projection referencePath = reference.accept(this);
                String field = fieldAccess.getField().getName();

                return referencePath.append(field);
            }

            @Override
            public Projection visitSubQuery(RexSubQuery subQuery) {
                throw new ComplexExpressionException();
            }
        });
    }
}
