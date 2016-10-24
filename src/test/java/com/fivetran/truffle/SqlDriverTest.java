package com.fivetran.truffle;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

public class SqlDriverTest {
    @BeforeClass
    public static void registerDriver() {
        // Causes driver to register itself
        TruffleDriver.load();
    }

    @Test
    public void select1() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:truffle://localhost:80")) {
            ResultSet r = conn.createStatement().executeQuery(
                    "WITH test_values (id, attr) AS (" +
                    "  VALUES (1, 'one'), (2, 'two')" +
                    ") " +
                    "SELECT * FROM test_values"
            );
            List<Object[]> results = new ArrayList<>();

            while (r.next()) {
                Object[] row = {r.getLong("ID"), r.getString("ATTR")};

                results.add(row);
            }

            assertThat(results, containsInAnyOrder(new Object[][] {
                    {1L, "one"},
                    {2L, "two"}
            }));
        }
    }
}
