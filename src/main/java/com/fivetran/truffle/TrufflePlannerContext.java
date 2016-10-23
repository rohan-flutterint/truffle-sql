package com.fivetran.truffle;

import org.apache.calcite.plan.Context;

public class TrufflePlannerContext implements Context {
    @Override
    public <T> T unwrap(Class<T> clazz) {
        return null;
    }
}
