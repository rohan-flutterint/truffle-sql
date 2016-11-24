package com.fivetran.truffle.parse;

import com.fivetran.truffle.NamedProjection;
import com.fivetran.truffle.Projection;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.*;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
        paths(fieldNames, project).ifPresent(projections -> {
            PhysicalParquet pushdown = scan.withProject(projections);

            call.transformTo(pushdown);
        });
    }

    private static Optional<List<NamedProjection>> paths(List<String> fieldNames, PhysicalProject project) {
        List<NamedProjection> acc = new ArrayList<>();

        for (Pair<RexNode, String> each : project.getNamedProjects()) {
            RexNode exp = each.getKey();
            String name = each.getValue();
            Optional<Projection> projection = path(fieldNames, exp);

            // If expression can't be represented as a simple projection, give up and return empty
            if (!projection.isPresent())
                return Optional.empty();
            // Otherwise add it to the list
            else
                acc.add(new NamedProjection(name, projection.get()));
        }

        return Optional.of(acc);
    }

    /**
     * If exp looks like a.b.c, return Projection.of(a, b, c);
     * otherwise return Projection.of()
     */
    private static Optional<Projection> path(List<String> fieldNames, RexNode exp) {
        return exp.accept(new RexVisitor<Optional<Projection>>() {
            @Override
            public Optional<Projection> visitInputRef(RexInputRef inputRef) {
                String head = fieldNames.get(inputRef.getIndex());

                return Optional.of(Projection.of(head));
            }

            @Override
            public Optional<Projection> visitLocalRef(RexLocalRef localRef) {
                return Optional.empty();
            }

            @Override
            public Optional<Projection> visitLiteral(RexLiteral literal) {
                return Optional.empty();
            }

            @Override
            public Optional<Projection> visitCall(RexCall call) {
                return Optional.empty();
            }

            @Override
            public Optional<Projection> visitOver(RexOver over) {
                return Optional.empty();
            }

            @Override
            public Optional<Projection> visitCorrelVariable(RexCorrelVariable correlVariable) {
                return Optional.empty();
            }

            @Override
            public Optional<Projection> visitDynamicParam(RexDynamicParam dynamicParam) {
                return Optional.empty();
            }

            @Override
            public Optional<Projection> visitRangeRef(RexRangeRef rangeRef) {
                return Optional.empty();
            }

            @Override
            public Optional<Projection> visitFieldAccess(RexFieldAccess fieldAccess) {
                RexNode reference = fieldAccess.getReferenceExpr();
                Optional<Projection> referencePath = reference.accept(this);
                String field = fieldAccess.getField().getName();

                return referencePath.map(path -> path.append(field));
            }

            @Override
            public Optional<Projection> visitSubQuery(RexSubQuery subQuery) {
                return null;
            }
        });
    }
}
