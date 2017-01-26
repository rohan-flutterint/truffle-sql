package com.fivetran.truffle.parse;

import com.fivetran.truffle.compile.RowSource;
import com.fivetran.truffle.compile.ThenRowSink;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

class PhysicalAggregate extends Aggregate implements PhysicalRel {

    static PhysicalAggregate convert(LogicalAggregate from) {
        return new PhysicalAggregate(
                from.getCluster(),
                from.getTraitSet(),
                from.getInput(),
                from.indicator,
                from.getGroupSet(),
                from.getGroupSets(),
                from.getAggCallList()
        );
    }

    private PhysicalAggregate(RelOptCluster cluster,
                                RelTraitSet traits,
                                RelNode child,
                                boolean indicator,
                                ImmutableBitSet groupSet,
                                List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet,
                          RelNode input,
                          boolean indicator,
                          ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets,
                          List<AggregateCall> aggCalls) {
        return new PhysicalAggregate(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls);
    }

    @Override
    public RowSource compile(ThenRowSink next) {
        assert getGroupSets() == null : "groupSets is not supported";

        PhysicalRel input = (PhysicalRel) getInputs().get(0);

        throw new UnsupportedOperationException();
        // return RelAggregate.compile(input, next, getGroupSet(), getAggCallList());
    }
}
