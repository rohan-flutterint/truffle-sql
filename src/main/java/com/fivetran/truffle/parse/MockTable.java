package com.fivetran.truffle.parse;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

public class MockTable extends AbstractTable implements TranslatableTable {
    private final Class<?> type;
    private final Object[] rows;

    public MockTable(Class<?> type, Object[] rows) {
        this.type = type;
        this.rows = rows;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        TruffleTypeFactory factory = (TruffleTypeFactory) typeFactory;

        return factory.createPeekableStructType(type);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable table) {
        return new PhysicalMock(context.getCluster(), context.getCluster().traitSet().replace(PhysicalRel.CONVENTION), table, type, rows);
    }
}
