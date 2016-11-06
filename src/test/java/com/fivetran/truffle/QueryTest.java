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
        mockRows = new Object[]{
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
        mockRows = new Object[]{
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

    public static class IdNested {
        public final int id;
        public final Nested nested;

        public IdNested(int id, int nestedX, int nestedY) {
            this.id = id;
            this.nested = new Nested(nestedX, nestedY);
        }

        public IdNested(int id) {
            this.id = id;
            this.nested = null;
        }
    }

    public static class Nested {
        public final int x, y;

        public Nested(int x, int y) {
            this.x = x;
            this.y = y;
        }
    }

    @Test
    public void nestedType() throws SQLException {
        mockRows = new Object[]{
                new IdNested(1, 2, 3),
                new IdNested(4, 5, 6),
                new IdNested(10)
        };

        List<Object[]> results = query("SELECT m.id, m.nested.x, m.nested.y FROM TABLE(mock()) AS m");

        assertThat(results, containsInAnyOrder(new Object[][] {
                {1L, 2L, 3L},
                {4L, 5L, 6L},
                {10L, null, null}
        }));
    }

    @Test
    public void unqualifiedNestedType() throws SQLException {
        mockRows = new Object[]{
                new IdNested(1, 2, 3),
                new IdNested(4, 5, 6),
                new IdNested(10)
        };

        List<Object[]> results = query("SELECT id, nested.x, nested.y FROM TABLE(mock())");

        assertThat(results, containsInAnyOrder(new Object[][] {
                {1L, 2L, 3L},
                {4L, 5L, 6L},
                {10L, null, null}
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
