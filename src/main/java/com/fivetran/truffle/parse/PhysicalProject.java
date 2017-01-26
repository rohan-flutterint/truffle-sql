package com.fivetran.truffle.parse;

import com.fivetran.truffle.compile.RelEmpty;
import com.fivetran.truffle.compile.RelProject;
import com.fivetran.truffle.compile.RowSource;
import com.fivetran.truffle.compile.ThenRowSink;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

class PhysicalProject extends Project implements PhysicalRel {

    static PhysicalRel convert(LogicalProject from) {
        return new PhysicalProject(
                from.getCluster(),
                from.getTraitSet().replace(CONVENTION),
                RelOptRule.convert(from.getInput(), CONVENTION),
                from.getProjects(),
                from.getRowType()
        );
    }

    private PhysicalProject(RelOptCluster cluster,
                    RelTraitSet traits,
                    RelNode input,
                    List<? extends RexNode> projects,
                    RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);

        assert getConvention() == PhysicalRel.CONVENTION;
        assert getConvention() == input.getConvention();
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new PhysicalProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public RowSource compile(ThenRowSink last) {
        return compileInput(sourceFrame -> RelProject.compile(sourceFrame, getProjects(), last));
    }

    private RowSource compileInput(ThenRowSink next) {
        // Multiple inputs should never occur in LogicalProject, only in other RelNode implementers like MultiJoin
        assert getInputs().size() <= 1 : "Project has " + getInputs().size() + " inputs";

        RowSource compiled;

        if (getInputs().isEmpty()) {
            compiled = RelEmpty.compile(next);
        }
        else {
            PhysicalRel input = (PhysicalRel) getInputs().get(0);

            compiled = input.compile(next);
        }

        return compiled;
    }

}
