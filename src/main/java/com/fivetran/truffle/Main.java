package com.fivetran.truffle;

import com.fivetran.truffle.compile.TruffleSqlLanguage;
import com.fivetran.truffle.parse.TruffleMeta;
import com.oracle.truffle.api.CallTarget;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.intellij.lang.annotations.Language;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.logging.LogManager;

public class Main {
    static {
        try {
            InputStream properties = Parquets.class.getResourceAsStream("/logging.properties");

            LogManager.getLogManager().readConfiguration(properties);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void init() {
        // Causes static section to be invoked
    }

    public static void main(String[] args) throws SQLException {
        // Parse and run a query using Calcite JDBC implementation
        try {
            Class.forName("com.fivetran.truffle.TruffleDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:truffle://localhost:80")) {
            ResultSet r = conn.createStatement().executeQuery("SELECT 'Hello world!' AS message");

            while (r.next()) {
                String message = r.getString("message");

                System.out.println(message);
            }
        }

        // Parse and run a query without JDBC
        RelRoot plan = parse("SELECT 'Hello again!' AS message");
        CallTarget executable = compile(plan, row -> {
            for (Object column : row)
                System.out.print(column + "\t");

            System.out.println();
        });

        executable.call();
    }

    /**
     * Demonstrates how to parse and plan a query with minimal Calcite infrastructure.
     * This works right now because we have no database schema.
     * In the future, that will have to be an additional argument.
     */
    public static RelRoot parse(@Language("SQL") String query) {
        SqlNode parse = TruffleMeta.parse(query);

        return TruffleMeta.plan(parse);
    }

    /**
     * Demonstrates how to compile a query plan to an executable program.
     */
    public static CallTarget compile(RelRoot plan, Consumer<Object[]> forEachRow) {
        return TruffleSqlLanguage.INSTANCE.compileInteractiveQuery(plan, forEachRow);
    }
}
