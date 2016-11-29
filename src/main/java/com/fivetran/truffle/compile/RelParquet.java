package com.fivetran.truffle.compile;

import com.fivetran.truffle.NamedProjection;
import com.fivetran.truffle.Parquets;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;

import java.net.URI;
import java.util.List;

/**
 * A parquet file that can be read and sent to a SQL query
 */
public class RelParquet extends RowSource {
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
     * Flatten each field of this relation
     */
    @Children
    private final StatementWriteLocal[] writers;

    /**
     * Where we'll store the ColumnReaders that point to the current file
     */
    private final FrameDescriptorPart columnReaders;

    /**
     * Where we'll store the fields of this relation
     */
    private final FrameDescriptorPart sourceFrame;

    @Child
    protected RowSink then;

    public RelParquet(URI file, MessageType schema, List<NamedProjection> project) {
        this.file = file;
        this.schema = schema;
        this.project = project;
        this.writers = new StatementWriteLocal[project.size()];
        this.columnReaders = FrameDescriptorPart.root(project.size());
        this.sourceFrame = columnReaders.push(project.size());

        // Check that we are only projecting primitive columns
        for (NamedProjection each : project) {
            assert schema.getType(each.projection.path).isPrimitive() : "Can't project group at " + each.projection + " " + schema.getType(each.projection.path);
        }

        for (int i = 0; i < project.size(); i++) {
            // Fetches the ColumnReader from VirtualFrame
            ExprReadLocal columnReader = ExprReadLocalNodeGen.create(columnReaders.findFrameSlot(i));
            // Reads 1 value from Parquet file
            ExprReadColumn reader = ExprReadColumnNodeGen.create(schema, project.get(i).projection, columnReader);
            // Writes 1 value to VirtualFrame
            StatementWriteLocal writer = StatementWriteLocalNodeGen.create(reader, sourceFrame.findFrameSlot(i));

            writers[i] = writer;
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

            // Install each ColumnReader into frame
            ColumnReadStore readStore = Parquets.columns(file, schema, footer);

            for (int i = 0; i < project.size(); i++) {
                NamedProjection path = project.get(i);
                ColumnDescriptor column = schema.getColumnDescription(path.projection.path);
                ColumnReader columnReader = readStore.getColumnReader(column);
                FrameSlot slot = columnReaders.findFrameSlot(i);

                frame.setObject(slot, columnReader);
            }

            // Assemble each row
            for (long row = 0; row < nRows; row++) {
                writeRow(frame);
            }
        }
    }

    @TruffleBoundary
    private List<Footer> footers() {
        return Parquets.footers(file);
    }

    @ExplodeLoop
    private void writeRow(VirtualFrame frame) {
        for (StatementWriteLocal each : writers) {
            each.executeVoid(frame);
        }

        then.executeVoid(frame);
    }

    @Override
    public void bind(LazyRowSink next) {
        if (then == null)
            then = next.apply(sourceFrame);
        else
            then.bind(next);
    }
}
