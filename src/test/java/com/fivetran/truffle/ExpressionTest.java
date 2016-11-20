package com.fivetran.truffle;

import org.junit.Test;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import static org.hamcrest.Matchers.*;
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
    public void withClause() throws SQLException {
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
    public void forgiveNulls() throws SQLException {
        List<Object[]> results = query("SELECT 1 - null, 1 + null, null * 1, null / 1, true AND null, false OR null, 1 = null, 1 <> null");

        assertThat(results, not(empty()));
    }

    @Test
    public void add() throws SQLException {
        List<Object[]> results = query("SELECT 1 + 10 + 11, 1 + null");

        assertThat(results, contains(new Object[][] {
                {22L, null}
        }));
    }

    @Test
    public void subtract() throws SQLException {
        List<Object[]> results = query("SELECT 10 - 2 - 1, 1 - null");

        assertThat(results, contains(new Object[][] {
                {7L, null}
        }));
    }

    @Test
    public void multiply() throws SQLException {
        List<Object[]> results = query("SELECT 10 * 2, 1 * null");

        assertThat(results, contains(new Object[][] {
                {20L, null}
        }));
    }

    @Test
    public void divide() throws SQLException {
        List<Object[]> results = query("SELECT 10 / 2, null / 10");

        assertThat(results, contains(new Object[][] {
                {5L, null}
        }));
    }

    @Test
    public void and() throws SQLException {
        List<Object[]> results = query("SELECT true AND false, true AND null");

        assertThat(results, contains(new Object[][] {
                {false, null}
        }));
    }

    @Test
    public void or() throws SQLException {
        List<Object[]> results = query("SELECT true OR false, true OR null");

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

        results = query("SELECT 1 = null, cast(null AS INTEGER) = null");

        assertThat(results, contains(new Object[][] {
                {null, null}
        }));
    }

    @Test
    public void notEquals() throws SQLException {
        List<Object[]> results = query("SELECT 1 <> 1, 1.0 <> 1.0, 'one' <> 'one'");

        assertThat(results, contains(new Object[][] {
                {false, false, false}
        }));

        results = query("SELECT 1 <> 2, 1.0 <> 2.0, 'one' <> 'two'");

        assertThat(results, contains(new Object[][] {
                {true, true, true}
        }));

        results = query("SELECT 1 <> null, cast(null AS INTEGER) <> null");

        assertThat(results, contains(new Object[][] {
                {null, null}
        }));
    }

    @Test
    public void caseWhen() throws SQLException {
        List<Object[]> results = query("SELECT CASE 'foo' WHEN 'bar' THEN 1 WHEN 'foo' then 2 ELSE 0 END AS n");

        assertThat(results, contains(new Object[][] {
                {2L}
        }));
    }
}
