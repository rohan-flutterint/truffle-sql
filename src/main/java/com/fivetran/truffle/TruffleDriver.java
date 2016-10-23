package com.fivetran.truffle;

import org.apache.calcite.avatica.*;

import java.sql.DriverManager;
import java.sql.SQLException;

public class TruffleDriver extends UnregisteredDriver {
    static {
        try {
            DriverManager.registerDriver(new TruffleDriver());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void load() {
        // Causes static section to be invoked
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return new DriverVersion("Truffle JDBC driver", "0.1", "Truffle", "0.1", true, 0, 1, 0, 1);
    }

    @Override
    protected String getConnectStringPrefix() {
        return "jdbc:truffle:";
    }

    @Override
    public Meta createMeta(AvaticaConnection connection) {
        return new TruffleMeta(connection);
    }

}
