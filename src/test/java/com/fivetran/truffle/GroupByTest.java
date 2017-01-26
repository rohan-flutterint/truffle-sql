package com.fivetran.truffle;

import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class GroupByTest extends SqlTestBase {
    @Test
    public void groupByInt() throws SQLException {
        mockRows = random(100);

        List<Object[]> rows = query("SELECT `smallInt`, avg(smallDouble) FROM TABLE(mock()) GROUP BY `smallInt`");

        assertThat(rows, not(empty()));
    }

    private Object[] random(int n) {
        Object[] result = new Object[n];

        for (int i = 0; i < n; i++) {
            result[i] = new Types();
        }

        return result;
    }

    private static final Random RANDOM = new Random();

    public static class Types {
        public final long smallInt = RANDOM.nextInt(1);
        public final double smallDouble = RANDOM.nextDouble();
    }
}
