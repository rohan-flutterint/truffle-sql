package com.fivetran.truffle.parse;

import org.apache.calcite.rel.logical.LogicalUnion;

class RuleConvertUnion extends RuleConvert<LogicalUnion> {
    static final RuleConvertUnion INSTANCE = new RuleConvertUnion();

    private RuleConvertUnion() {
        super(LogicalUnion.class, RuleConvertUnion.class.getSimpleName());
    }
    
    @Override
    protected PhysicalRel doConvert(LogicalUnion union) {
        return PhysicalUnion.convert(union);
    }

}
