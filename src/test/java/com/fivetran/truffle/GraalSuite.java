package com.fivetran.truffle;

import com.fivetran.truffle.compile.ForeignAccess;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        DynamicTypeTest.class,
        ExpressionTest.class,
        FileTest.class,
        ForeignAccess.class,
        JoinTest.class,
        MockTest.class,
        OrderByTest.class,
        ParquetTest.class,
        RuleProjectParquetTest.class,
        RunTest.class
})
public class GraalSuite {
}
