package com.fivetran.truffle;

import com.oracle.truffle.api.dsl.TypeSystem;

@TypeSystem({boolean.class, long.class, double.class, String.class, SqlNull.class})
public class SqlTypes {
}
