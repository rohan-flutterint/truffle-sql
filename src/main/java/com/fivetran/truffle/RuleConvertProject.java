package com.fivetran.truffle;

import org.apache.calcite.rel.logical.LogicalProject;

/**
 * Replaces LogicalProject with TProject
 */
class RuleConvertProject extends RuleConvert<LogicalProject> {
    static RuleConvertProject INSTANCE = new RuleConvertProject();

    private RuleConvertProject() {
        super(LogicalProject.class, RuleConvertProject.class.getSimpleName());
    }

    @Override
    protected PhysicalRel doConvert(LogicalProject from) {
        return new PhysicalProject(
                from.getCluster(),
                from.getTraitSet().replace(PhysicalRel.CONVENTION),
                convert(from.getInput(), PhysicalRel.CONVENTION),
                from.getProjects(),
                from.getRowType()
        );
    }
}

