package com.fivetran.truffle;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

/**
 * Replaces LogicalProject with TProject
 */
public class RuleReplaceLogicalProject extends ConverterRule {
    public static RuleReplaceLogicalProject INSTANCE = new RuleReplaceLogicalProject();

    private RuleReplaceLogicalProject() {
        super(LogicalProject.class, Convention.NONE, TRel.CONVENTION, RuleReplaceLogicalProject.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalProject project = (LogicalProject) rel;

        return new TProject(
                project.getCluster(),
                project.getTraitSet().replace(TRel.CONVENTION),
                convert(project.getInput(), TRel.CONVENTION),
                project.getProjects(),
                project.getRowType()
        );
    }
}
