package com.fivetran.truffle.parse;

import org.apache.calcite.rel.logical.LogicalSort;

class RuleConvertSort extends RuleConvert<LogicalSort> {
    static final RuleConvertSort INSTANCE = new RuleConvertSort(LogicalSort.class);

    private RuleConvertSort(Class<LogicalSort> from) {
        super(from, RuleConvertSort.class.getSimpleName());
    }

    @Override
    protected PhysicalRel doConvert(LogicalSort logicalSort) {
        return PhysicalSort.convert(logicalSort);
    }
}
