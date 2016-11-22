package com.fivetran.truffle;

import com.fivetran.truffle.parse.TruffleTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ParquetsTest extends TestBase {
    @Test
    public void peekNestedFields() {
        // parent { a { b { c INT64 } } }
        PrimitiveType c = Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("c");
        GroupType b = Types.buildGroup(Type.Repetition.REQUIRED).addField(c).named("b");
        GroupType a = Types.buildGroup(Type.Repetition.REQUIRED).addField(b).named("a");
        MessageType parent = Types.buildMessage().addField(a).named("parent");

        // Root type does NOT have PEEK_FIELDS
        // PEEK_FIELDS means "you can reference this field without qualifying it"
        // The root is the table name, so no need for PEEK_FIELDS
        RelDataType sqlType = Parquets.sqlType(parent, new TruffleTypeFactory());

        assertThat(sqlType.getStructKind(), equalTo(StructKind.FULLY_QUALIFIED));

        // First layer of fields has PEEK_FIELDS
        // That way we can reference SELECT a.b.c FROM parent, rather than SELECT parent.a.b.c
        RelDataType aSql = sqlType.getField("a", true, false).getType();

        assertThat(aSql.getStructKind(), equalTo(StructKind.PEEK_FIELDS));

        // Subsequent layers do not
        // We don't want to be able to do SELECT b.c FROM parent - that would be bizarre
        RelDataType bSql = aSql.getField("b", true, false).getType();

        assertThat(bSql.getStructKind(), equalTo(StructKind.FULLY_QUALIFIED));

        // c is not a struct at all
        RelDataType cSql = bSql.getField("c", true, false).getType();

        assertThat(cSql.getStructKind(), equalTo(StructKind.NONE));
    }
}
