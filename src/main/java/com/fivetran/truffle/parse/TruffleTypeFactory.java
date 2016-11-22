package com.fivetran.truffle.parse;

import org.apache.calcite.jdbc.JavaRecordType;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Customizes the Calcite type system.
 */
public class TruffleTypeFactory extends JavaTypeFactoryImpl {
    @Override
    public RelDataType createType(Type type) {
        // ANY type
        if (type == Object.class)
            return createSqlType(SqlTypeName.ANY);
        else
            return super.createType(type);
    }


    public RelDataType createPeekableStructType(Class<?> type) {
        List<RelDataTypeField> list = createStructType(type)
                .getFieldList()
                .stream()
                .map(this::withPeek)
                .collect(Collectors.toList());

        return canonize(new JavaRecordType(list, type));
    }

    private RelDataTypeField withPeek(RelDataTypeField field) {
        if (field.getType().getStructKind() == StructKind.NONE)
            return field;
        else {
            FieldInfoBuilder result = builder();

            for (RelDataTypeField each : field.getType().getFieldList())
                result.add(each);

            RelDataType type = result.kind(StructKind.PEEK_FIELDS).build();

            return new RelDataTypeFieldImpl(field.getName(), field.getIndex(), type);
        }
    }
}
