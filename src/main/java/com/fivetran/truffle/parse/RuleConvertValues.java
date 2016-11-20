package com.fivetran.truffle.parse;

import org.apache.calcite.rel.logical.LogicalValues;

class RuleConvertValues extends RuleConvert<LogicalValues> {
    static RuleConvertValues INSTANCE = new RuleConvertValues();

    private RuleConvertValues() {
        super(LogicalValues.class, RuleConvertValues.class.getSimpleName());
    }

    @Override
    protected PhysicalRel doConvert(LogicalValues values) {
        return new PhysicalValues(
                values.getCluster(),
                values.getRowType(),
                values.getTuples(),
                values.getTraitSet().replace(PhysicalRel.CONVENTION)
        );
    }
}
