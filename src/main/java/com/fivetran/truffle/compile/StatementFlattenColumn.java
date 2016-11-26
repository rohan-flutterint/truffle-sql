package com.fivetran.truffle.compile;

import com.fivetran.truffle.Projection;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.schema.MessageType;

class StatementFlattenColumn extends StatementFlatten {
    /**
     * All operations are delegated to this child, which specializes.
     *
     * It's not convenient to specialize StatementFlattenColumn
     * because it doesn't use execute*(VirtualFrame) to do the parts that require specialization.
     */
    @Child
    private StatementReadColumn read;

    StatementFlattenColumn(MessageType schema, Projection path, FrameSlot slot) {
        read = new StatementReadColumn(schema, path, slot);
    }

    @Override
    void prepare(ColumnReadStore file) {
        read.prepare(file);
    }

    @Override
    void read(VirtualFrame frame) {
        read.executeVoid(frame);
    }

    @Override
    void consumeRepeats(VirtualFrame frame) {
        while (read.repeats(read.targetRepetitionLevel())) {
            // Read next value of column into frame
            read(frame);

            // Execute query on current row
            then.executeVoid(frame);
        }
    }

    @Override
    boolean repeats(int targetRepetitionLevel) {
        return read.repeats(targetRepetitionLevel);
    }

    @Override
    public void bind(RowSink next) {
        then = next;
    }
}
