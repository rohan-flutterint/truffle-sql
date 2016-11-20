package com.fivetran.truffle;

import org.apache.calcite.rel.logical.LogicalValues;

public class RuleConvertValues extends RuleConvert<LogicalValues> {
    public static RuleConvertValues INSTANCE = new RuleConvertValues();

    private RuleConvertValues() {
        super(LogicalValues.class, RuleConvertValues.class.getSimpleName());
    }

    @Override
    protected TRel doConvert(LogicalValues values) {
        return new TValues(
                values.getCluster(),
                values.getRowType(),
                values.getTuples(),
                values.getTraitSet().replace(TRel.CONVENTION)
        );
    }
}
