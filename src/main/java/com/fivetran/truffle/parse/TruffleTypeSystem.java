package com.fivetran.truffle.parse;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

class TruffleTypeSystem extends RelDataTypeSystemImpl {
    public static final TruffleTypeSystem INSTANCE = new TruffleTypeSystem();

    private TruffleTypeSystem() { }

    @Override
    public int getDefaultPrecision(SqlTypeName typeName) {
        switch (typeName) {
            case CHAR:
            case BINARY:
            case VARCHAR:
            case VARBINARY:
                return RelDataType.PRECISION_NOT_SPECIFIED;
            default:
                return super.getDefaultPrecision(typeName);
        }
    }
}
