package com.fivetran.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.SourceSection;
import org.apache.calcite.rel.logical.LogicalProject;

import java.util.List;

class RelProject extends RowTransform {
    private final ExprBase[] select;
    private final FrameDescriptor resultFrame;

    RelProject(SourceSection source, LogicalProject project, RootNode then) {
        super(source, sourceFrame(project), then);

        this.select = project.getChildExps()
                .stream()
                .map(child -> child.accept(new CompileExpr(getFrameDescriptor())))
                .toArray(ExprBase[]::new);
        this.resultFrame = Types.frame(project.getRowType());
    }

    private static FrameDescriptor sourceFrame(LogicalProject project) {
        if (project.getInputs().isEmpty())
            return new FrameDescriptor();
        else
            return Types.frame(project.getInputs().get(0).getRowType());
    }

    @Override
    public Object execute(VirtualFrame frame) {
        List<? extends FrameSlot> slots = resultFrame.getSlots();
        VirtualFrame thenFrame = Truffle.getRuntime().createVirtualFrame(new Object[]{}, resultFrame);

        for (int column = 0; column < select.length; column++) {
            Object value = select[column].execute(frame);
            FrameSlot slot = slots.get(column);

            thenFrame.setObject(slot, value);
        }

        then.execute(thenFrame);

        return QueryReturn.INSTANCE;
    }

    @Override
    FrameDescriptor getResultFrameDescriptor() {
        return resultFrame;
    }
}
