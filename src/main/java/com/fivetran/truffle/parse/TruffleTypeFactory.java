package com.fivetran.truffle.parse;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Type;

/**
 * Customizes the Calcite type system.
 */
public class TruffleTypeFactory extends JavaTypeFactoryImpl {
    public TruffleTypeFactory() {
        super(TruffleTypeSystem.INSTANCE);
    }

    @Override
    public RelDataType createType(Type type) {
        // ANY type
        if (type == Object.class)
            return createSqlType(SqlTypeName.ANY);
        else
            return super.createType(type);
    }

}
