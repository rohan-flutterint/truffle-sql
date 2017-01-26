package com.fivetran.truffle.parse;

import org.apache.calcite.rel.logical.LogicalAggregate;

class RuleConvertAggregate extends RuleConvert<LogicalAggregate> {
    static final RuleConvertAggregate INSTANCE = new RuleConvertAggregate();

    private RuleConvertAggregate() {
        super(LogicalAggregate.class, RuleConvertAggregate.class.getSimpleName());
    }

    @Override
    protected PhysicalAggregate doConvert(LogicalAggregate aggregate) {
        return PhysicalAggregate.convert(aggregate);
    }
}
