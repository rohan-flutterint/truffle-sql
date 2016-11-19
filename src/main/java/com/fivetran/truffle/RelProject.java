package com.fivetran.truffle;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;

/**
 * The expression part of a SELECT query
 */
public class RelProject extends RowTransform {
    @Children
    private final StatementWriteLocal[] select;

    private final FrameDescriptorPart frame;

    RelProject(FrameDescriptorPart sourceFrame, LogicalProject project) {
        super();

        this.select = new StatementWriteLocal[project.getChildExps().size()];
        this.frame = sourceFrame.push(select.length);

        for (int i = 0; i < project.getChildExps().size(); i++) {
            RexNode child = project.getChildExps().get(i);
            FrameSlot slot = frame.findFrameSlot(i);
            ExprBase compiled = compile(sourceFrame, child);

            select[i] = StatementWriteLocalNodeGen.create(compiled, slot);
        }
    }

    private ExprBase compile(FrameDescriptorPart sourceFrame, RexNode child) {
        CompileExpr compiler = new CompileExpr(sourceFrame);

        return child.accept(compiler);
    }

    @Override
    @ExplodeLoop
    public void executeVoid(VirtualFrame frame) {
        for (StatementWriteLocal each : select)
            each.executeVoid(frame);

        then.executeVoid(frame);
    }

    @Override
    public FrameDescriptorPart frame() {
        return frame;
    }
}
