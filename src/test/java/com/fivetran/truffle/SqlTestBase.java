package com.fivetran.truffle;

import com.fivetran.truffle.parse.MockTable;
import com.fivetran.truffle.parse.ParquetTable;
import com.fivetran.truffle.parse.TruffleMeta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.schema.MessageType;
import org.intellij.lang.annotations.Language;
import org.junit.After;
import org.junit.BeforeClass;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public abstract class SqlTestBase extends TestBase {
    @BeforeClass
    public static void registerDriver() throws ClassNotFoundException {
        // Causes driver to register itself
        Class.forName("com.fivetran.truffle.TruffleDriver");

        // TODO this compensates for the fact that Avatica demands we convert Instant to long,
        // then interprets that long as local-timezone
        // We should fix that issue and then this won't be necessary
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    protected static Object[] mockRows;

    @BeforeClass
    public static void registerTableMacros() {
        // mock() returns whatever is currently in SqlTestBase.mockRows
        TruffleMeta.registerMacro("mock", new TableMacro() {
            @Override
            public TranslatableTable apply(List<Object> arguments) {
                Objects.requireNonNull(mockRows, "You need to set MockTest.mockRows before calling TABLE(mock())");

                return new MockTable(mockRows[0].getClass(), mockRows);
            }

            @Override
            public List<FunctionParameter> getParameters() {
                return Collections.emptyList();
            }
        });

        // file('/path/to/file.parquet') reads parquet file from local disk
        TruffleMeta.registerMacro("file", new TableMacro() {
            @Override
            public TranslatableTable apply(List<Object> arguments) {
                String file = (String) arguments.get(0);

                assert file.startsWith("file://") : "Only local files are supported";

                URI uri = URI.create(file);
                List<Footer> footers = Parquets.footers(uri);

                if (footers.isEmpty())
                    throw new RuntimeException("No footers in " + uri);

                // TODO this only reads 1 footer, a parquet file could have multiple contradictory footers
                MessageType schema = footers.get(0).getParquetMetadata().getFileMetaData().getSchema();

                return new ParquetTable(uri, schema);
            }

            @Override
            public List<FunctionParameter> getParameters() {
                return Collections.singletonList(
                        new FunctionParameter() {
                            @Override
                            public int getOrdinal() {
                                return 0;
                            }

                            @Override
                            public String getName() {
                                return "path";
                            }

                            @Override
                            public RelDataType getType(RelDataTypeFactory typeFactory) {
                                return typeFactory.createJavaType(String.class);
                            }

                            @Override
                            public boolean isOptional() {
                                return false;
                            }
                        }
                );
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
            int nColumns = r.getMetaData().getColumnCount();

            while (r.next()) {
                Object[] row = new Object[nColumns];

                for (int column = 0; column < nColumns; column++) {
                    row[column] = r.getObject(column + 1);
                }

                results.add(row);
            }

        }

        return results;
    }
}
