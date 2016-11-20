package com.fivetran.truffle;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Rules that convert Logical* to Physical*
 */
public abstract class RuleConvert<From extends RelNode> extends ConverterRule {
    private final Class<From> fromClass;

    public RuleConvert(Class<From> from,
                       String description) {
        super(from, Convention.NONE, PhysicalRel.CONVENTION, description);

        this.fromClass = from;
    }

    @Override
    public final PhysicalRel convert(RelNode rel) {
        return doConvert(fromClass.cast(rel));
    }

    protected abstract PhysicalRel doConvert(From cast);
}
