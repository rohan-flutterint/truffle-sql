package com.fivetran.truffle;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;

/**
 * Scans a parquet-format file.
 */
public class ParquetTableScan extends TableScan {
    protected ParquetTableScan(RelOptCluster cluster,
                               RelTraitSet traitSet,
                               RelOptTable table) {
        super(cluster, traitSet, table);
    }

    @Override
    public void register(RelOptPlanner planner) {
        // TODO push down projections, filters
    }
}
