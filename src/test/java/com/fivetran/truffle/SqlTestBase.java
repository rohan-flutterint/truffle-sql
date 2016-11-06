package com.fivetran.truffle;

import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.intellij.lang.annotations.Language;
import org.junit.After;
import org.junit.BeforeClass;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SqlTestBase {
    @BeforeClass
    public static void registerDriver() {
        // Causes driver to register itself
        TruffleDriver.load();
    }

    protected static Object[] mockRows;

    @BeforeClass
    public static void registerTableMacros() {
        TruffleMeta.registerMacro("mock", new TableMacro() {
            @Override
            public TranslatableTable apply(List<Object> arguments) {
                Objects.requireNonNull(mockRows, "You need to set QueryTest.mockRows before calling TABLE(mock())");

                return new MockTable(mockRows[0].getClass(), mockRows);
            }

            @Override
            public List<FunctionParameter> getParameters() {
                return Collections.emptyList();
            }
        });
    }

    @After
    public void resetMockRows() {
        mockRows = null;
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
