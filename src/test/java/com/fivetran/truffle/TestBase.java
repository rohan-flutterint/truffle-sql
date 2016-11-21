package com.fivetran.truffle;

import org.junit.BeforeClass;

public class TestBase {
    @BeforeClass
    public static void setLogFormat() {
        Main.init();
    }
}
