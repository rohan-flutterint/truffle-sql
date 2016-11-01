package com.fivetran.truffle;

import org.intellij.lang.annotations.Language;
import org.junit.After;
import org.junit.BeforeClass;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SqlTestBase {
    @BeforeClass
    public static void registerDriver() {
        // Causes driver to register itself
        TruffleDriver.load();
    }

    @After
    public void resetMockRows() {
        TruffleMeta.mockRows = null;
    }

    protected static List<Object[]> query(@Language("SQL") String sql) throws SQLException {
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
