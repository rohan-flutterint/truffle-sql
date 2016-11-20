package com.fivetran.truffle;

import com.fivetran.truffle.parse.TruffleTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ParquetsTest {
    @Test
    public void peekNestedFields() {
        GroupType nested = Types.buildGroup(Type.Repetition.REQUIRED)
                .addField(Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("foo"))
                .addField(Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("bar"))
                .named("nested");
        GroupType parent = Types.buildGroup(Type.Repetition.REQUIRED)
                .addField(nested)
                .named("parent");
        RelDataType sqlType = Parquets.sqlType(parent, new TruffleTypeFactory());

        assertThat(sqlType.getStructKind(), equalTo(StructKind.FULLY_QUALIFIED));

        RelDataType nestedSqlType = sqlType.getField("nested", true, false).getType();

        assertThat(nestedSqlType.getStructKind(), equalTo(StructKind.PEEK_FIELDS));
    }
}
