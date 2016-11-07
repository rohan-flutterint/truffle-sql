package com.fivetran.truffle;

import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static com.fivetran.truffle.ParquetTestResources.documentPath;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class FileTest extends SqlTestBase {
    @Test
    public void selectColumn() throws IOException, SQLException {
        List<Object[]> rows = query("SELECT docId, `name`.url FROM TABLE(file('" + documentPath() + "'))");

        assertThat(rows, contains(new Object[][] {
                {10, "http://A"},
                {10, "http://B"},
                {10, null},
                {20, "http://C"}
        }));
    }
}
