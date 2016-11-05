package com.fivetran.truffle;

import com.oracle.truffle.api.interop.ForeignAccess;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.object.DynamicObject;
import com.oracle.truffle.api.object.ObjectType;

public class SqlObjectType extends ObjectType {

    public static final ObjectType INSTANCE = new SqlObjectType();

    private SqlObjectType() { }

    public static boolean isInstance(TruffleObject obj) {
        return TruffleSqlContext.isSqlObject(obj);
    }

    @Override
    public ForeignAccess getForeignAccessFactory(DynamicObject obj) {
        throw new UnsupportedOperationException(); // TODO
    }
}
