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
    public void selectStar() throws SQLException {
        Object[] rows = {
                new IdName(1, "one"),
                new IdName(2, "two")
        };

        withFake(rows, () -> {
            List<Object[]> results = query("SELECT * FROM test_schema.test_table");

            assertThat(results, containsInAnyOrder(new Object[][] {
                    {1L, "one"},
                    {2L, "two"}
            }));
        });
    }

    @Test
    public void selectColumn() throws SQLException {
        Object[] rows = {
                new IdName(1, "one"),
                new IdName(2, "two")
        };

        withFake(rows, () -> {
            List<Object[]> results = query("SELECT id FROM test_schema.test_table");

            assertThat(results, containsInAnyOrder(new Object[][] {
                    {1L},
                    {2L}
            }));
        });

        withFake(rows, () -> {
            List<Object[]> results = query("SELECT name FROM test_schema.test_table");

            assertThat(results, containsInAnyOrder(new Object[][] {
                    {"one"},
                    {"two"}
            }));
        });
    }

    @Test
    public void tableMacro() throws SQLException {
        List<Object[]> rows = query("SELECT * FROM TABLE(echo('Hello world!'))");

        assertThat(rows, containsInAnyOrder(new Object[][] {
                {"Hello world!"}
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
