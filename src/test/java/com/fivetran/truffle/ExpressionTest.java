package com.fivetran.truffle;

import org.junit.Test;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/**
 * Tests SQL expressions such as literals, binary operators.
 */
public class ExpressionTest extends SqlTestBase {
    @Test
    public void withExpression() throws SQLException {
        List<Object[]> results = query(
                "WITH test_values (id, attr) AS (" +
                "  VALUES (1, 'one'), (2, 'two')" +
                ") " +
                "SELECT * FROM test_values"
        );

        assertThat(results, containsInAnyOrder(new Object[][] {
                {1L, "one"},
                {2L, "two"}
        }));
    }

    @Test
    public void nullValue() throws SQLException {
        List<Object[]> results = query(
                "WITH test_values (x, y) AS (" +
                "  VALUES (1, 10), (2, cast(null as INTEGER)) " +
                ") " +
                "SELECT x, y, x + y " +
                "FROM test_values"
        );

        assertThat(results, contains(new Object[][] {
                {1L, 10L, 11L},
                {2L, null, null}
        }));
    }

    @Test
    public void literals() throws SQLException {
        List<Object[]> results = query("SELECT 1, 1.0, 'one', DATE '2016-01-01', TIMESTAMP '2016-01-01 00:00:00'");

        assertThat(results, contains(new Object[][] {
                {1L, 1.0d, "one", Date.valueOf("2016-01-01"), Timestamp.valueOf("2016-01-01 00:00:00")}
        }));
    }

    @Test
    public void add() throws SQLException {
        List<Object[]> results = query("SELECT 1 + 10 + 11, 1 + cast(null AS INTEGER)");

        assertThat(results, contains(new Object[][] {
                {22L, null}
        }));
    }

    @Test
    public void subtract() throws SQLException {
        List<Object[]> results = query("SELECT 10 - 2 - 1, 1 - cast(null AS INTEGER)");

        assertThat(results, contains(new Object[][] {
                {7L, null}
        }));
    }

    @Test
    public void multiply() throws SQLException {
        List<Object[]> results = query("SELECT 10 * 2, 1 * cast(null AS INTEGER)");

        assertThat(results, contains(new Object[][] {
                {20L, null}
        }));
    }

    @Test
    public void divide() throws SQLException {
        List<Object[]> results = query("SELECT 10 / 2, cast(null AS INTEGER) / 10");

        assertThat(results, contains(new Object[][] {
                {5L, null}
        }));
    }

    @Test
    public void and() throws SQLException {
        List<Object[]> results = query("SELECT true AND false, true AND cast(null AS BOOLEAN)");

        assertThat(results, contains(new Object[][] {
                {false, null}
        }));
    }

    @Test
    public void or() throws SQLException {
        List<Object[]> results = query("SELECT true OR false, true OR cast(null AS BOOLEAN)");

        assertThat(results, contains(new Object[][] {
                {true, true}
        }));
    }

    @Test
    public void equals() throws SQLException {
        List<Object[]> results = query("SELECT 1 = 1, 1.0 = 1.0, 'one' = 'one'");

        assertThat(results, contains(new Object[][] {
                {true, true, true}
        }));

        results = query("SELECT 1 = 2, 1.0 = 2.0, 'one' = 'two'");

        assertThat(results, contains(new Object[][] {
                {false, false, false}
        }));

        results = query("SELECT 1 = cast(null AS INTEGER), cast(null AS INTEGER) = cast(null AS INTEGER)");

        assertThat(results, contains(new Object[][] {
                {false, false}
        }));
    }
}
