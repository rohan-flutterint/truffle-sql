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
    public void literal() throws SQLException {
        List<Object[]> results = query(
                "WITH test_values (id, attr) AS (" +
                "  VALUES (1, 'one'), (2, 'two')" +
                ") " +
                "SELECT * FROM test_values"
        );

        assertThat(results, containsInAnyOrder(new Object[][] {
                {1, "one"},
                {2, "two"}
        }));
    }

    @Test
    public void select1() throws SQLException {
        List<Object[]> results = query("SELECT 1, 'one'");

        assertThat(results, contains(new Object[][] {
                {1, "one"}
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
