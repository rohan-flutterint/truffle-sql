package com.fivetran.truffle.parse;

import org.apache.calcite.rel.logical.LogicalProject;

/**
 * Replaces LogicalProject with TProject
 */
class RuleConvertProject extends RuleConvert<LogicalProject> {
    static final RuleConvertProject INSTANCE = new RuleConvertProject();

    private RuleConvertProject() {
        super(LogicalProject.class, RuleConvertProject.class.getSimpleName());
    }

    @Override
    protected PhysicalRel doConvert(LogicalProject from) {
        return PhysicalProject.convert(from);
    }

}

