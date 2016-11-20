package com.fivetran.truffle.compile;

import com.oracle.truffle.api.interop.ForeignAccess;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.object.DynamicObject;
import com.oracle.truffle.api.object.ObjectType;

/**
 * Defines the `ObjectType` of any `DynamicObject` we create from Truffle-SQL, for example to represent a nested field.
 */
class SqlObjectType extends ObjectType {

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
