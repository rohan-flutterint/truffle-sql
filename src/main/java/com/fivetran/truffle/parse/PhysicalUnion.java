package com.fivetran.truffle.parse;

import com.fivetran.truffle.compile.RelUnion;
import com.fivetran.truffle.compile.RowSource;
import com.fivetran.truffle.compile.ThenRowSink;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;

import java.util.List;
import java.util.stream.Collectors;

class PhysicalUnion extends Union implements PhysicalRel {

    static PhysicalRel convert(LogicalUnion union) {
        List<RelNode> inputs = union.getInputs()
                .stream()
                .map(i -> RelOptRule.convert(i, CONVENTION))
                .collect(Collectors.toList());

        return new PhysicalUnion(
                union.getCluster(),
                union.getTraitSet().replace(CONVENTION),
                inputs,
                union.all
        );
    }

    private PhysicalUnion(RelOptCluster cluster,
                            RelTraitSet traits,
                            List<RelNode> inputs,
                            boolean all) {
        super(cluster, traits, inputs, all);

        for (RelNode each : inputs) {
            assert each.getConvention() == CONVENTION;
        }
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        assert traitSet.containsIfApplicable(CONVENTION);

        return new PhysicalUnion(getCluster(), traitSet, inputs, all);
    }

    @Override
    public RowSource compile(ThenRowSink next) {
        // TODO it would probably be better to treat `next` as a function and invoke it
        // I think this effectively copies the `next` expression into each part of the union
        RowSource[] sources = getInputs()
                .stream()
                .map(i -> ((PhysicalRel) i).compile(next))
                .toArray(RowSource[]::new);

        return new RelUnion(sources);
    }
}
