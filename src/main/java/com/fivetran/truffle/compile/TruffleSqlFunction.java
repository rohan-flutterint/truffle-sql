package com.fivetran.truffle.compile;

import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.interop.ForeignAccess;
import com.oracle.truffle.api.interop.TruffleObject;

/**
 * Represents a Truffle SQL function, which we can make callable from Java using
 * {@link com.oracle.truffle.api.interop.java.JavaInterop#asJavaFunction(Class, TruffleObject)}
 *
 * Based on SLFunction
 */
class TruffleSqlFunction implements TruffleObject {
    /**
     * Implementation of the function
     */
    final RootCallTarget callTarget;

    TruffleSqlFunction(RootCallTarget callTarget) {
        this.callTarget = callTarget;
    }

    @Override
    public ForeignAccess getForeignAccess() {
        return TruffleSqlMessageResolutionForeign.createAccess();
    }
}
