package com.fivetran.truffle.parse;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;

import java.lang.reflect.Type;

public class TruffleTypeFactory extends JavaTypeFactoryImpl {
    @Override
    public RelDataType createType(Type type) {
        RelDataType result = super.createType(type);

        // Allow unqualified nested field references like SELECT nested.x.y FROM foo
        // instead of having to always do SELECT foo.nested.x.y
        if (result instanceof RelRecordType)
            return new RelRecordType(StructKind.PEEK_FIELDS, result.getFieldList());
        else
            return result;
    }
}
