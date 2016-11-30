package com.fivetran.truffle;

import com.google.common.base.Throwables;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static com.fivetran.truffle.ParquetTestResources.*;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class FileTest extends SqlTestBase {
    /**
     * Do a simple addition
     */
    @Test
    public void primitive() throws IOException, SQLException {
        List<Object[]> rows = query("SELECT docId + 1 FROM TABLE(file('" + documentPath() + "'))");

        assertThat(rows, contains(new Object[][] {
                {11L},
                {21L}
        }));
    }

    /**
     * We don't attempt to auto-destructure repeated columns.
     * Calcite (and the SQL standard) treat these like a table embedded in another table:
     * you need to deal with them in the FROM clause.
     */
    @Test
    public void repeated() throws IOException, SQLException {
        try {
            query("SELECT t.`name`.`language`.`code` FROM TABLE(file('" + documentPath() + "')) AS t");
        } catch (SQLException e) {
            assertThat(Throwables.getRootCause(e).getMessage(), containsString("not found"));

            return;
        }

        fail("Repeated column should not have been found");
    }

    /**
     * Can we reference a nested column in a parquet file?
     */
    @Test
    public void nestedSingleColumn() throws IOException, SQLException {
        List<Object[]> rows = query("SELECT t.cases.`upper` FROM TABLE(file('" + simplePath() + "')) AS t");

        assertThat(rows, contains(new Object[][] {
                {"ONE"},
                {"TWO"},
                {null}
        }));
    }

    /**
     * If we WERE doing auto-destructuring using flattening, this would be the result
     */
    @Test
    @Ignore
    public void multipleRepeatedColumns() throws IOException, SQLException {
        List<Object[]> rows = query("SELECT docId, t.`name`.url, t.`name`.`language`.code FROM TABLE(file('" + documentPath() + "')) AS t");

        assertThat(rows, contains(new Object[][] {
                {10L, "http://A", "en-us"},
                {10L, "http://A", "en"},
                {10L, "http://B", null},
                {10L, null, "en-gb"},
                {20L, "http://C", null}
        }));
    }

    /**
     * Reference a nested column in a complicated, indirect way
     */
    @Test
    @Ignore // https://issues.apache.org/jira/browse/CALCITE-1518
    public void nestedTwoStage() throws IOException, SQLException {
        List<Object[]> rows = query("SELECT t.cases.`upper` " +
                                    "FROM (SELECT cases FROM TABLE(file('" + simplePath() + "'))) AS t");

        assertThat(rows, contains(new Object[][] {
                {10L, "http://A"},
                {10L, "http://B"},
                {10L, null},
                {20L, "http://C"}
        }));
    }

    /**
     * nested.value and nestedPlus.value must get different names in intermediate expressions
     */
    @Test
    public void ambiguousNested() throws IOException, SQLException {
        List<Object[]> rows = query("SELECT id, t.nested.`value`, t.nestedPlus.`value` FROM TABLE(file('" + trickyPath() + "')) AS t");

        assertThat(rows, contains(new Object[][] {
                {1L, 10L, 11L},
                {2L, 20L, 21L},
                {3L, null, null},
        }));
    }
}
