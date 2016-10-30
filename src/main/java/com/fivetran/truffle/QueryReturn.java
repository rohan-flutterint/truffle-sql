package com.fivetran.truffle;

/**
 * The value returned by queries when you call {@link com.oracle.truffle.api.RootCallTarget#call}
 */
public class QueryReturn {
    private QueryReturn() { }

    public static final QueryReturn INSTANCE = new QueryReturn();
}
