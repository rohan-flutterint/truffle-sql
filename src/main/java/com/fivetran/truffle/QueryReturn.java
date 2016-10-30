package com.fivetran.truffle;

import com.oracle.truffle.api.interop.ForeignAccess;
import com.oracle.truffle.api.interop.TruffleObject;

/**
 * The value returned by queries when you call {@link com.oracle.truffle.api.RootCallTarget#call}
 */
public class QueryReturn implements TruffleObject {
    private QueryReturn() { }

    public static final QueryReturn INSTANCE = new QueryReturn();

    @Override
    public ForeignAccess getForeignAccess() {
        return null; // TODO
    }
}
