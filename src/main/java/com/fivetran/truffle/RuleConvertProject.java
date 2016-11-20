package com.fivetran.truffle;

import org.apache.calcite.rel.logical.LogicalProject;

/**
 * Replaces LogicalProject with TProject
 */
public class RuleConvertProject extends RuleConvert<LogicalProject> {
    public static RuleConvertProject INSTANCE = new RuleConvertProject();

    private RuleConvertProject() {
        super(LogicalProject.class, RuleConvertProject.class.getSimpleName());
    }

    @Override
    protected TRel doConvert(LogicalProject from) {
        return new TProject(
                from.getCluster(),
                from.getTraitSet().replace(TRel.CONVENTION),
                convert(from.getInput(), TRel.CONVENTION),
                from.getProjects(),
                from.getRowType()
        );
    }
}

