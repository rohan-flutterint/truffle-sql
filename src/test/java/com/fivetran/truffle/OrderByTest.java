package com.fivetran.truffle;

import org.junit.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class OrderByTest extends SqlTestBase {
    @Test
    public void orderByInt() throws SQLException {
        mockRows = random(100);

        List<Object[]> rows = query("SELECT `smallInt` FROM TABLE(mock()) AS t ORDER BY `smallInt`");

        assertThat(rows, not(empty()));

        Object[] lastRow = rows.get(0);

        for (int i = 1; i < rows.size(); i++) {
            Object[] thisRow = rows.get(i);
            Long lastInt = (Long) lastRow[0];
            Long thisInt = (Long) thisRow[0];

            assertThat(thisInt, greaterThanOrEqualTo(lastInt));

            lastRow = thisRow;
        }
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
        public final long smallInt = RANDOM.nextInt(100);
        public final double smallDouble = RANDOM.nextDouble();
    }
}
