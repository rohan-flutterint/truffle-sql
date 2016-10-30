package com.fivetran.truffle;

import com.oracle.truffle.api.dsl.TypeSystem;

import java.time.Instant;
import java.time.LocalDate;

@TypeSystem({boolean.class, long.class, double.class, LocalDate.class, Instant.class, String.class, SqlNull.class})
public class SqlTypes {
}
