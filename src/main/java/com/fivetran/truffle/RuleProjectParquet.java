package com.fivetran.truffle;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.parquet.schema.MessageType;

import java.util.Objects;

public class RuleProjectParquet extends RelOptRule {
    public static final RuleProjectParquet INSTANCE = new RuleProjectParquet();

    private RuleProjectParquet() {
        super(operand(LogicalProject.class, operand(ParquetTableScan.class, none())), "RuleProjectParquet");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        ParquetTableScan scan = call.rel(1);
        MessageType smallerType = messageType(scan.schema, project);

        // When the query contains an expression like SELECT a+1 FROM tbl, onMatch gets called multiple times
        //
        // The first time it has just the references, for example SELECT a FROM tbl
        // We push the references down into ParquetTableScan
        //
        // The second time it has the expressions, for example SELECT a+1 FROM tbl[a]
        // There's nothing more we can do with expressions, so we do nothing
        if (smallerType != null) {
            ParquetTableScan pushdown = scan.withProject(smallerType);

            call.transformTo(pushdown);
        }
    }

    private MessageType messageType(MessageType schema, LogicalProject project) {
        // We use null to represent the empty message type, since MessageType prohibits no-fields
        MessageType union = null;

        for (Pair<RexNode, String> p : project.getNamedProjects()) {
            // We can only handle direct reference to inputs
            if (!(p.getKey() instanceof RexInputRef))
                return null;

            String fieldName = p.getValue();
            MessageType part = new MessageType(schema.getName(), schema.getType(fieldName));

            union = union(union, part);
        }

        Objects.requireNonNull(union, "MessageType is empty");

        return union;
    }

    private MessageType union(MessageType union, MessageType part) {
        if (union == null)
            return part;
        else if (part == null)
            return union;
        else
            return union.union(part);
    }
}
