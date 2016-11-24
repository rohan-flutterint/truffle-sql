package com.fivetran.truffle.compile;

import com.fivetran.truffle.NamedProjection;
import com.fivetran.truffle.Parquets;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;

import java.net.URI;
import java.util.List;

/**
 * A parquet file that can be read and sent to a SQL query
 */
public class RelParquet extends RowSourceSimple {
    /**
     * File we are reading
     */
    private final URI file;

    /**
     * Original schema of the file
     */
    private final MessageType schema;

    /**
     * Columns we want to project
     */
    private final List<NamedProjection> project;

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

    public RelParquet(URI file, MessageType schema, List<NamedProjection> project) {
        super(FrameDescriptorPart.root(project.size()));

        this.file = file;
        this.schema = schema;
        this.project = project;
        this.readers = new ExprAssemble[project.size()];
        this.writers = new StatementWriteLocal[project.size()];

        for (int i = 0; i < project.size(); i++) {
            String[] path = project.get(i).projection.path;
            ExprAssemble reader = CompileParquet.compile(schema, path);
            FrameSlot slot = sourceFrame.findFrameSlot(i);

            readers[i] = reader;
            writers[i] = StatementWriteLocalNodeGen.create(reader, slot);
        }
    }

    @Override
    protected void executeVoid() {
        VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(new Object[]{}, sourceFrame.frame());

        for (Footer footer : footers()) {
            // Count rows
            long nRows = 0;

            for (BlockMetaData each : footer.getParquetMetadata().getBlocks()) {
                nRows += each.getRowCount();
            }

            // Install ColumnReadStore into each column reader
            ColumnReadStore readStore = Parquets.columns(file, schema, footer);

            for (ExprAssemble reader : readers) {
                reader.prepare(readStore);
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
