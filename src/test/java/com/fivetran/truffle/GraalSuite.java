package com.fivetran.truffle;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        DynamicTypeTest.class,
        ExpressionTest.class,
        FileTest.class,
        JoinTest.class,
        MockTest.class,
        ParquetTest.class,
        RuleProjectParquetTest.class,
        RunTest.class
})
public class GraalSuite {
}
