package com.fivetran.truffle;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.Test;

import java.io.IOException;

import static com.fivetran.truffle.ParquetTestResources.simplePath;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.Assert.assertThat;

public class RuleProjectParquetTest extends SqlTestBase {

    @Test
    public void projectNested() throws IOException {
        RelRoot parse = Main.parse("SELECT t.cases.`upper` FROM TABLE(file('" + simplePath() + "')) AS t");
        RelDataType type = parse.rel.getRowType();

        assertThat(type, hasToString("RecordType(VARCHAR upper)"));
    }
}
