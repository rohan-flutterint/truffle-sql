package com.fivetran.truffle.compiler;

/**
 * Internal representation of Null
 */
public class NullValue  {
    public static final NullValue INSTANCE = new NullValue();

    private NullValue() { }
}
