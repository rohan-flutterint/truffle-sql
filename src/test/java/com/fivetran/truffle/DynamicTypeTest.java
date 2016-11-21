package com.fivetran.truffle;

import org.junit.Test;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class DynamicTypeTest extends SqlTestBase {
    @Test
    public void caseWhen() throws SQLException {
        mockRows = new Object[]{
                new Something("foo")
        };

        List<Object[]> results = query("SELECT CASE WHEN cast(something AS BOOLEAN) THEN 1 ELSE 0 END FROM TABLE(mock())");

        assertThat(results, contains(new Object[][] {
                {0L}
        }));
    }

    @Test
    public void castBoolean() throws SQLException {
        mockRows = new Object[] {
                new Something(true),
                new Something("foo"),
                new Something(null)
        };

        List<Object[]> results = query("SELECT cast(something AS BOOLEAN) FROM TABLE(mock())");

        assertThat(results, contains(new Object[][] {
                {true},
                {null},
                {null}
        }));
    }

    @Test
    public void castLong() throws SQLException {
        mockRows = new Object[] {
                new Something(1L),
                new Something(2.1d),
                new Something("foo"),
                new Something(null)
        };

        List<Object[]> results = query("SELECT cast(something AS BIGINT) FROM TABLE(mock())");

        assertThat(results, contains(new Object[][] {
                {1L},
                {2L},
                {null},
                {null}
        }));
    }

    @Test
    public void castDouble() throws SQLException {
        mockRows = new Object[] {
                new Something(1.0d),
                new Something(2L),
                new Something("foo"),
                new Something(null)
        };

        List<Object[]> results = query("SELECT cast(something AS DOUBLE) FROM TABLE(mock())");

        assertThat(results, contains(new Object[][] {
                {1.0d},
                {2.0d},
                {null},
                {null}
        }));
    }

    @Test
    public void castVarchar() throws SQLException {
        mockRows = new Object[] {
                new Something("hi"),
                new Something(1L),
                new Something(2.0d),
                new Something(null)
        };

        List<Object[]> results = query("SELECT cast(something AS VARCHAR) FROM TABLE(mock())");

        assertThat(results, contains(new Object[][] {
                {"hi"},
                {"1"},
                {"2.0"},
                {null}
        }));
    }

    @Test
    public void castDate() throws SQLException {
        LocalDate date = LocalDate.of(2016, 1, 1);

        mockRows = new Object[] {
                new Something(date),
                new Something("foo"),
                new Something(null)
        };

        List<Object[]> results = query("SELECT cast(something AS DATE) FROM TABLE(mock())");

        assertThat(results, contains(new Object[][] {
                {Date.valueOf(date)},
                {null},
                {null}
        }));
    }

    @Test
    public void castTimestamp() throws SQLException {
        Instant date = LocalDate.of(2016, 1, 1).atStartOfDay().atOffset(ZoneOffset.UTC).toInstant();

        mockRows = new Object[] {
                new Something(date),
                new Something("foo"),
                new Something(null)
        };

        List<Object[]> results = query("SELECT cast(something AS TIMESTAMP) FROM TABLE(mock())");
        Timestamp expected = Timestamp.from(date);

        assertThat(results, contains(new Object[][] {
                {expected},
                {null},
                {null}
        }));
    }

    public static class Something {
        public final Object something;

        public Something(Object something) {
            this.something = something;
        }
    }
}
