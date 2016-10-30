package com.fivetran.truffle;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

class TruffleTable extends AbstractTable implements TranslatableTable {
    private final JavaTypeFactory types;

    public TruffleTable(JavaTypeFactory types) {
        this.types = types;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        // TODO actually get types from somewhere
        return types.createStructType(TruffleMeta.TestRow.class);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable table) {
        return new LogicalTableScan(context.getCluster(), context.getCluster().traitSet(), table);
    }
}
