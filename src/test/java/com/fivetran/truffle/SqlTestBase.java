package com.fivetran.truffle;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.intellij.lang.annotations.Language;
import org.junit.BeforeClass;

import java.sql.*;
import java.util.*;
import java.util.function.Function;

public class SqlTestBase {
    @BeforeClass
    public static void registerDriver() {
        // Causes driver to register itself
        TruffleDriver.load();
    }


    @FunctionalInterface
    protected interface RunTest {
        void run() throws SQLException;
    }

    protected void withFake(Object[] rows, RunTest test) throws SQLException {
        Function<TruffleMeta, Prepare.CatalogReader> realCatalog = TruffleMeta.catalogReader;

        try {
            TruffleMeta.catalogReader = any -> {
                JavaTypeFactory types = TruffleMeta.typeFactory();
                AbstractTable mockTable = new MockTable(rows[0].getClass(), rows);
                AbstractSchema mockSchema = new AbstractSchema() {
                    @Override
                    protected Map<String, Table> getTableMap() {
                        return Collections.singletonMap(
                                "test_table", mockTable
                        );
                    }
                };
                CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);

                rootSchema.add("test_schema", mockSchema);

                return new CalciteCatalogReader(rootSchema, true, Collections.emptyList(), types);
            };

            test.run();
        } finally {
            Objects.requireNonNull(realCatalog);

            TruffleMeta.catalogReader = realCatalog;
        }
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
