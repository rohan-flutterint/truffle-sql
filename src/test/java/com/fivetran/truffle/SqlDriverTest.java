package com.fivetran.truffle;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

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
                    "SELECT * FROM test_schema.test_table"
            );

            while (r.next()) {
                System.out.println(r.getInt("id") + "\t" + r.getString("attr"));
            }
        }
    }
}
