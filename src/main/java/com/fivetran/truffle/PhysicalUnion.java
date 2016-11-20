package com.fivetran.truffle;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;

import java.util.List;

class PhysicalUnion extends Union implements PhysicalRel {
    protected PhysicalUnion(RelOptCluster cluster,
                            RelTraitSet traits,
                            List<RelNode> inputs,
                            boolean all) {
        super(cluster, traits, inputs, all);

        for (RelNode each : inputs) {
            assert each.getConvention() == PhysicalRel.CONVENTION;
        }
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        assert traitSet.containsIfApplicable(PhysicalRel.CONVENTION);

        return new PhysicalUnion(getCluster(), traitSet, inputs, all);
    }

    @Override
    public RowSource compile() {
        RowSource[] sources = getInputs()
                .stream()
                .map(i -> ((PhysicalRel) i).compile())
                .toArray(RowSource[]::new);

        return new RelUnion(sources);
    }
}
