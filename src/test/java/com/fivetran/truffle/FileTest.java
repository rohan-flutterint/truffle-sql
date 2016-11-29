package com.fivetran.truffle;

import com.google.common.base.Throwables;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static com.fivetran.truffle.ParquetTestResources.documentPath;
import static com.fivetran.truffle.ParquetTestResources.simplePath;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class FileTest extends SqlTestBase {
    @Test
    public void primitive() throws IOException, SQLException {
        List<Object[]> rows = query("SELECT docId + 1 FROM TABLE(file('" + documentPath() + "'))");

        assertThat(rows, contains(new Object[][] {
                {11L},
                {21L}
        }));
    }

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

    @Test
    public void nestedSingleColumn() throws IOException, SQLException {
        List<Object[]> rows = query("SELECT t.cases.`upper` FROM TABLE(file('" + simplePath() + "')) AS t");

        assertThat(rows, contains(new Object[][] {
                {"ONE"},
                {"TWO"},
                {null}
        }));
    }

    @Test
    @Ignore // Not doing auto-destructuring
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

    @Test
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
}
