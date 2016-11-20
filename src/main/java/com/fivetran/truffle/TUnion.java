package com.fivetran.truffle;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;

import java.util.List;

public class TUnion extends Union implements TRel {
    protected TUnion(RelOptCluster cluster,
                     RelTraitSet traits,
                     List<RelNode> inputs,
                     boolean all) {
        super(cluster, traits, inputs, all);

        for (RelNode each : inputs) {
            assert each.getConvention() == TRel.CONVENTION;
        }
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        assert traitSet.containsIfApplicable(TRel.CONVENTION);

        return new TUnion(getCluster(), traitSet, inputs, all);
    }

    @Override
    public RowSource compile() {
        RowSource[] sources = getInputs()
                .stream()
                .map(i -> ((TRel) i).compile())
                .toArray(RowSource[]::new);

        return new RelUnion(sources);
    }
}
