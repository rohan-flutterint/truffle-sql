package com.fivetran.truffle;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class JoinTest extends SqlTestBase {
    @Test
    @Ignore
    public void notIn() throws SQLException {
        MockTest.mockRows = new Object[] {
                new Id(1),
                new Id(2)
        };
        List<Object[]> results = query("SELECT id FROM TABLE(mock()) WHERE id NOT IN (SELECT id + 1 FROM TABLE(mock()))");

        assertThat(results, contains(new Object[][] {
                {2L}
        }));
    }

    public static class Id {
        public final long id;

        public Id(long id) {
            this.id = id;
        }
    }
}
