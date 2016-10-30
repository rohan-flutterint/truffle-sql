package com.fivetran.truffle;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class SqlDriverTest {
    @BeforeClass
    public static void registerDriver() {
        // Causes driver to register itself
        TruffleDriver.load();
    }

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
    public void literals() throws SQLException {
        List<Object[]> results = query("SELECT 1, 1.0, 'one', DATE '2016-01-01', TIMESTAMP '2016-01-01 00:00:00'");

        assertThat(results, contains(new Object[][] {
                {1L, 1.0d, "one", Date.valueOf("2016-01-01"), Timestamp.valueOf("2016-01-01 00:00:00")}
        }));
    }

    @Test
    public void add() throws SQLException {
        List<Object[]> results = query("SELECT 1 + 10 + 11 AS result_value");

        assertThat(results, contains(new Object[][] {
                {22L}
        }));
    }

    @Test
    public void subtract() throws SQLException {
        List<Object[]> results = query("SELECT 10 - 2 - 1 AS result_value");

        assertThat(results, contains(new Object[][] {
                {7L}
        }));
    }

    @Test
    public void multiply() throws SQLException {
        List<Object[]> results = query("SELECT 10 * 2 AS result_value");

        assertThat(results, contains(new Object[][] {
                {20L}
        }));
    }

    @Test
    public void divide() throws SQLException {
        List<Object[]> results = query("SELECT 10 / 2 AS result_value");

        assertThat(results, contains(new Object[][] {
                {5L}
        }));
    }

    @Test
    public void and() throws SQLException {
        List<Object[]> results = query("SELECT TRUE AND FALSE AS result_value");

        assertThat(results, contains(new Object[][] {
                {false}
        }));
    }

    @Test
    public void or() throws SQLException {
        List<Object[]> results = query("SELECT TRUE OR FALSE AS result_value");

        assertThat(results, contains(new Object[][] {
                {true}
        }));
    }

    @Test
    public void equals() throws SQLException {
        List<Object[]> results = query("SELECT 1 = 1, 1.0 = 1.0, 'one' = 'one' AS result_value");

        assertThat(results, contains(new Object[][] {
                {true, true, true}
        }));

        results = query("SELECT 1 = 2, 1.0 = 2.0, 'one' = 'two' AS result_value");

        assertThat(results, contains(new Object[][] {
                {false, false, false}
        }));

        results = query("SELECT 1 = cast(null AS INTEGER), cast(null AS INTEGER) = cast(null AS INTEGER) AS result_value");

        assertThat(results, contains(new Object[][] {
                {false, false}
        }));
    }

    private List<Object[]> query(String sql) throws SQLException {
        List<Object[]> results = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection("jdbc:truffle://localhost:80")) {
            ResultSet r = conn.createStatement().executeQuery(sql);
            ResultSetMetaData types = r.getMetaData();
            int n = types.getColumnCount();

            while (r.next()) {
                Object[] row = new Object[n];

                for (int column = 0; column < n; column++) {
                    row[column] = r.getObject(column + 1);
                }

                results.add(row);
            }

        }
        return results;
    }
}
