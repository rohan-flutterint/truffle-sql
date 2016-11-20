package com.fivetran.truffle;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;

import java.util.List;
import java.util.stream.Collectors;

public class RuleConvertUnion extends RuleConvert<LogicalUnion> {
    public static RuleConvertUnion INSTANCE = new RuleConvertUnion();

    private RuleConvertUnion() {
        super(LogicalUnion.class, RuleConvertUnion.class.getSimpleName());
    }
    
    @Override
    protected TRel doConvert(LogicalUnion union) {
        List<RelNode> inputs = union.getInputs()
                .stream()
                .map(i -> convert(i, TRel.CONVENTION))
                .collect(Collectors.toList());

        return new TUnion(
                union.getCluster(),
                union.getTraitSet().replace(TRel.CONVENTION),
                inputs,
                union.all
        );
    }
}
