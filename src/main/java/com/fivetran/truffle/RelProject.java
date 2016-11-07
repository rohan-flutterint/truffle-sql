package com.fivetran.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.source.SourceSection;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * The expression part of a SELECT query
 */
public class RelProject extends RowTransform {
    @Children
    private final ExprBase[] select;

    private final FrameDescriptor resultFrame;

    RelProject(SourceSection source, LogicalProject project, RowSink then) {
        super(source, sourceFrame(project), then);

        this.select = project.getChildExps()
                .stream()
                .map(this::compile)
                .toArray(ExprBase[]::new);
        this.resultFrame = Types.frame(project.getRowType());
    }

    private ExprBase compile(RexNode child) {
        CompileExpr compiler = new CompileExpr(sourceFrame);

        return child.accept(compiler);
    }

    private static FrameDescriptor sourceFrame(LogicalProject project) {
        if (project.getInputs().isEmpty())
            return new FrameDescriptor();
        else
            return Types.frame(project.getInputs().get(0).getRowType());
    }

    @Override
    public void executeVoid(VirtualFrame frame) {
        List<? extends FrameSlot> slots = resultFrame.getSlots();
        VirtualFrame thenFrame = Truffle.getRuntime().createVirtualFrame(new Object[]{}, resultFrame);

        // TODO length of loop is a compile-time constant, make sure it gets unrolled
        for (int column = 0; column < select.length; column++) {
            Object value = select[column].executeGeneric(frame);
            FrameSlot slot = slots.get(column);

            thenFrame.setObject(slot, value);
        }

        then.executeVoid(thenFrame);
    }

}
