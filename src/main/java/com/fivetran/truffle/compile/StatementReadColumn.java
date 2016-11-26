package com.fivetran.truffle.compile;

import com.fivetran.truffle.Parquets;
import com.fivetran.truffle.Projection;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.schema.MessageType;

/**
 * Reads a single Parquet column into VirtualFrame in a specialized way
 */
class StatementReadColumn extends StatementBase {
    protected final ColumnDescriptor column;
    protected final int targetRepetitionLevel;
    protected ColumnReader currentFile;

    /**
     * Reads column value in a specialized way.
     *
     * Not annotated with @Child because it's a child of writer.
     * We need the direct reference so that we can call reader.prepare(ColumnReader)
     */
    protected ExprReadColumn reader;

    /**
     * Writes column value in a specialized way.
     */
    @Child
    protected StatementWriteLocal writer;

    protected StatementReadColumn(MessageType schema, Projection path, FrameSlot slot) {
        this.column = schema.getColumnDescription(path.path);
        this.targetRepetitionLevel = Parquets.targetRepetitionLevel(schema, path);
        this.reader = ExprReadColumnNodeGen.create(schema, path);
        this.writer = StatementWriteLocalNodeGen.create(reader, slot);
    }

    void prepare(ColumnReadStore file) {
        currentFile = file.getColumnReader(column);

        reader.prepare(currentFile);
    }

    boolean repeats(int targetRepetitionLevel) {
        return reader.repeats(targetRepetitionLevel);
    }

    int targetRepetitionLevel() {
        return targetRepetitionLevel;
    }

    @Override
    void executeVoid(VirtualFrame frame) {
        writer.executeVoid(frame);
    }
}
