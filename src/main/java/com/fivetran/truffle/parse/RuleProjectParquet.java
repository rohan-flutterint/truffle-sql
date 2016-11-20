package com.fivetran.truffle.parse;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.parquet.schema.MessageType;

import java.util.Objects;

class RuleProjectParquet extends RelOptRule {
    static final RuleProjectParquet INSTANCE = new RuleProjectParquet();

    private RuleProjectParquet() {
        super(operand(PhysicalProject.class, operand(PhysicalParquet.class, none())), RuleProjectParquet.class.getSimpleName());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhysicalProject project = call.rel(0);
        PhysicalParquet scan = call.rel(1);
        MessageType smallerType = messageType(scan.schema, project);

        // When the query contains an expression like SELECT a+1 FROM tbl, onMatch gets called multiple times
        //
        // The first time it has just the references, for example SELECT a FROM tbl
        // We push the references down into ParquetTableScan
        //
        // The second time it has the expressions, for example SELECT a+1 FROM tbl[a]
        // There's nothing more we can do with expressions, so we do nothing
        if (smallerType != null) {
            PhysicalParquet pushdown = scan.withProject(smallerType);

            call.transformTo(pushdown);
        }
    }

    private MessageType messageType(MessageType schema, Project project) {
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
