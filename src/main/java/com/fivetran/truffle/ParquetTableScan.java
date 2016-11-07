package com.fivetran.truffle;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.parquet.schema.MessageType;

import java.net.URI;

/**
 * Scans a parquet-format file.
 */
class ParquetTableScan extends TableScan implements CompileRowSource {
    /**
     * Location of the file. Could be a local file, S3.
     */
    final URI file;

    /**
     * The schema we want to project from the file.
     * Might be changed by rules in RelOptPlanner to push down projections.
     */
    final MessageType schema;

    ParquetTableScan(RelOptCluster cluster,
                     RelTraitSet traitSet,
                     RelOptTable table,
                     URI file,
                     MessageType schema) {
        super(cluster, traitSet, table);

        this.file = file;
        this.schema = schema;
    }

    @Override
    public void register(RelOptPlanner planner) {
        // TODO push down projections, filters
    }

    @Override
    public RowSource compile(RowSink then) {
        return new RelParquet(file, schema, then);
    }
}
