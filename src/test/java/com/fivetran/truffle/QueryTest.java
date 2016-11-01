package com.fivetran.truffle;

import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * Test query expressions like FROM
 */
public class QueryTest extends SqlTestBase {

    @Test
    public void echoMacro() throws SQLException {
        TruffleMeta.mockRows = new Object[]{
                new IdName(1, "one"),
                new IdName(2, "two")
        };

        List<Object[]> results = query("SELECT * FROM TABLE(echo('Hello world!'))");

        assertThat(results, containsInAnyOrder(new Object[][] {
                {"Hello world!"}
        }));
    }

    @Test
    public void mockMacro() throws SQLException {
        TruffleMeta.mockRows = new Object[]{
                new IdName(1, "one"),
                new IdName(2, "two")
        };

        List<Object[]> results = query("SELECT * FROM TABLE(mock())");

        assertThat(results, containsInAnyOrder(new Object[][] {
                {1L, "one"},
                {2L, "two"}
        }));

        results = query("SELECT id FROM TABLE(mock())");

        assertThat(results, containsInAnyOrder(new Object[][] {
                {1L},
                {2L}
        }));

        results = query("SELECT name FROM TABLE(mock())");

        assertThat(results, containsInAnyOrder(new Object[][] {
                {"one"},
                {"two"}
        }));
    }

    private static class IdName {
        public final long id;
        public final String name;

        IdName(long id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}
