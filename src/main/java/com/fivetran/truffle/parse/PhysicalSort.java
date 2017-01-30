package com.fivetran.truffle.parse;

import com.fivetran.truffle.compile.ExternalSort;
import com.fivetran.truffle.compile.RowSource;
import com.fivetran.truffle.compile.ThenRowSink;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;

class PhysicalSort extends Sort implements PhysicalRel {
    private PhysicalSort(RelOptCluster cluster,
                         RelTraitSet traits,
                         RelNode child,
                         RelCollation collation,
                         RexNode offset,
                         RexNode fetch) {
        super(cluster, traits, child, collation, offset, fetch);
    }

    @Override
    public RowSource compile(ThenRowSink next) {
        return ExternalSort.compile((PhysicalRel) input, collation.getFieldCollations(), next);
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new PhysicalSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }

    public static PhysicalRel convert(LogicalSort from) {
        return new PhysicalSort(
                from.getCluster(),
                from.getTraitSet().replace(CONVENTION),
                RelOptRule.convert(from.getInput(), CONVENTION),
                from.getCollation(),
                from.offset,
                from.fetch
        );
    }
}
