package com.fivetran.truffle.compile;

import com.fivetran.truffle.Parquets;
import com.fivetran.truffle.Projection;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

import java.util.function.Function;

class StatementFlattenGroup extends StatementFlatten {
    @Children
    private final StatementFlatten[] children;

    /**
     * If every column in this group has repetitionLevel == target, then this group has repeated!
     * We can read all the columns again, leave any sibling columns as they are, and execute the query on a new row.
     */
    private final int targetRepetitionLevel;

    StatementFlattenGroup(MessageType schema, Projection path, Function<Projection, FrameSlot> findFrameSlot) {
        this.targetRepetitionLevel = Parquets.targetRepetitionLevel(schema, path);

        GroupType type = (GroupType) schema.getType(path.path);

        this.children = new StatementFlatten[type.getFieldCount()];

        for (int i = 0; i < type.getFieldCount(); i++) {
            Projection childPath = path.append(type.getFieldName(i));

            children[i] = compile(schema, childPath, findFrameSlot);
        }
    }

    @Override
    @ExplodeLoop
    void prepare(ColumnReadStore file) {
        for (StatementFlatten child : children) {
            child.prepare(file);
        }
    }

    @Override
    @ExplodeLoop
    void read(VirtualFrame frame) {
        for (StatementFlatten child : children) {
            child.read(frame);
        }
    }

    @Override
    @ExplodeLoop
    void consumeRepeats(VirtualFrame frame) {
        for (StatementFlatten child : children) {
            child.consumeRepeats(frame);
        }

        while (repeats(targetRepetitionLevel)) {
            read(frame);

            then.executeVoid(frame);
        }
    }

    @Override
    @ExplodeLoop
    boolean repeats(int targetRepetitionLevel) {
        for (StatementFlatten child : children) {
            if (child.repeats(targetRepetitionLevel))
                return true;
        }

        return false;
    }

    @Override
    void bind(RowSink next) {
        then = next;
    }
}
