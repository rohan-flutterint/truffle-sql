package com.fivetran.truffle;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class TProject extends Project implements TRel {
    TProject(RelOptCluster cluster,
             RelTraitSet traits,
             RelNode input,
             List<? extends RexNode> projects,
             RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);

        assert getConvention() == TRel.CONVENTION;
        assert getConvention() == input.getConvention();
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new TProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public RowSource compile() {
        RowSource input = compileInput();

        input.bind(sourceFrame -> new RelProject(sourceFrame, getChildExps()));

        return input;
    }

    private RowSource compileInput() {
        // Multiple inputs should never occur in LogicalProject, only in other RelNode implementers like MultiJoin
        assert getInputs().size() <= 1 : "Project has " + getInputs().size() + " inputs";

        RowSource compiled;

        if (getInputs().isEmpty())
            compiled = new RelEmpty();
        else {
            TRel input = (TRel) getInputs().get(0);

            compiled = input.compile();
        }
        return compiled;
    }
}
