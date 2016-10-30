package com.fivetran.truffle;

import com.oracle.truffle.api.interop.ForeignAccess;
import com.oracle.truffle.api.interop.TruffleObject;

public class SqlNull implements TruffleObject {
    public static final SqlNull INSTANCE = new SqlNull();

    private SqlNull() { }

    @Override
    public ForeignAccess getForeignAccess() {
        return null; // TODO
    }
}
