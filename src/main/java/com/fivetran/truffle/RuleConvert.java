package com.fivetran.truffle;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Rules that convert Logical* to T*
 */
public abstract class RuleConvert<From extends RelNode> extends ConverterRule {
    private final Class<From> fromClass;

    public RuleConvert(Class<From> from,
                       String description) {
        super(from, Convention.NONE, TRel.CONVENTION, description);

        this.fromClass = from;
    }

    @Override
    public final TRel convert(RelNode rel) {
        return doConvert(fromClass.cast(rel));
    }

    protected abstract TRel doConvert(From cast);
}
