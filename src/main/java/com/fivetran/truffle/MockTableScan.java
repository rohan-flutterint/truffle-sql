package com.fivetran.truffle;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;

public class MockTableScan extends TableScan {
    public final Class<?> type;
    public final Object[] rows;

    public MockTableScan(RelOptCluster cluster, RelTraitSet relTraits, RelOptTable table, Class<?> type, Object[] rows) {
        super(cluster, relTraits, table);

        this.type = type;
        this.rows = rows;
    }
}
