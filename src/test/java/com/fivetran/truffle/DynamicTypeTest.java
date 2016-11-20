package com.fivetran.truffle;

import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class DynamicTypeTest extends SqlTestBase {
    @Test
    public void caseWhen() throws SQLException {
        mockRows = new Object[]{
                new Something("foo")
        };

        List<Object[]> results = query("SELECT CASE WHEN cast(something AS BOOLEAN) THEN 1 ELSE 0 END FROM TABLE(mock())");

        assertThat(results, contains(new Object[][] {
                {0L}
        }));
    }

    public static class Something {
        public final Object something;

        public Something(Object something) {
            this.something = something;
        }
    }
}
