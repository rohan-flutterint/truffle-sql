package com.fivetran.truffle;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.parquet.schema.MessageType;

import java.net.URI;

class ParquetTable extends AbstractTable implements TranslatableTable {
    private final URI file;
    private final MessageType schema;

    ParquetTable(URI file, MessageType schema) {
        this.file = file;
        this.schema = schema;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return Parquets.sqlType(schema, typeFactory);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable table) {
        return new ParquetTableScan(context.getCluster(), context.getCluster().traitSet(), table, file, schema);
    }
}
