package com.fivetran.truffle;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

class TMock extends TableScan implements TRel {
    public final Class<?> type;
    public final Object[] rows;

    public TMock(RelOptCluster cluster, RelTraitSet relTraits, RelOptTable table, Class<?> type, Object[] rows) {
        super(cluster, relTraits, table);

        assert getConvention() == TRel.CONVENTION;

        this.type = type;
        this.rows = rows;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public RowSource compile() {
        return new RelMock(getRowType(), type, rows);
    }
}
