package com.fivetran.truffle;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        DynamicTypeTest.class,
        ExpressionTest.class,
        FileTest.class,
        MockTest.class,
        ParquetsTest.class,
        ParquetTest.class,
        RunTest.class
})
public class GraalSuite {
}
