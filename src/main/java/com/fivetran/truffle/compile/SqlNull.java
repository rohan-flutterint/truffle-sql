package com.fivetran.truffle.compile;

import com.oracle.truffle.api.interop.ForeignAccess;
import com.oracle.truffle.api.interop.TruffleObject;

/**
 * Our representation of NULL
 */
public class SqlNull implements TruffleObject {
    public static final SqlNull INSTANCE = new SqlNull();

    private SqlNull() { }

    @Override
    public ForeignAccess getForeignAccess() {
        return null; // TODO
    }

    @Override
    public String toString() {
        return "NULL";
    }
}
