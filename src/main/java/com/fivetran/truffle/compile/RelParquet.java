package com.fivetran.truffle.compile;

import com.fivetran.truffle.Parquets;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.schema.MessageType;

import java.net.URI;
import java.util.List;

/**
 */
public class RelParquet extends RowSourceSimple {
    /**
     * File we are reading
     */
    private final URI file;

    /**
     * Columns we want to project
     */
    private final MessageType project;

    /**
     * Column readers. These are children of writers,
     * but we need a direct reference so we can call {@link ExprAssemble#prepare(ColumnReadStore)}.
     */
    private final ExprAssemble[] readers;

    /**
     * A series of column = reader[i] statements
     */
    @Children
    private final StatementWriteLocal[] writers;

    public RelParquet(URI file, MessageType project) {
        super(FrameDescriptorPart.root(project.getFieldCount()));

        this.file = file;
        this.project = project;
        this.readers = new ExprAssemble[project.getFieldCount()];
        this.writers = new StatementWriteLocal[project.getFieldCount()];

        for (int i = 0; i < project.getFieldCount(); i++) {
            String[] path = {project.getFieldName(i)};
            ExprAssemble reader = CompileParquet.compile(project, path);
            FrameSlot slot = sourceFrame.findFrameSlot(i);

            readers[i] = reader;
            writers[i] = StatementWriteLocalNodeGen.create(reader, slot);
        }
    }

    @Override
    protected void executeVoid() {
        VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(new Object[]{}, sourceFrame.frame());

        for (Footer footer : footers()) {
            ColumnReadStore readStore = Parquets.columns(file, project, footer);

            // Prepare to read rows
            long nRows = 0;

            for (ExprAssemble reader : readers) {
                reader.prepare(readStore);

                // TODO use BlockMetaData
                if (nRows < reader.getTotalValueCount())
                    nRows = reader.getTotalValueCount();
            }

            // Assemble each row
            for (long row = 0; row < nRows; row++) {
                writeRow(frame);

                then.executeVoid(frame);
            }
        }
    }

    @TruffleBoundary
    private List<Footer> footers() {
        return Parquets.footers(file);
    }

    @ExplodeLoop
    private void writeRow(VirtualFrame frame) {
        for (StatementWriteLocal writer : writers)
            writer.executeVoid(frame);
    }
}
