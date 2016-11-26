package com.fivetran.truffle.compile;

import com.fivetran.truffle.NamedProjection;
import com.fivetran.truffle.Parquets;
import com.fivetran.truffle.Projection;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private final StatementFlatten[] readers;

    /**
     * Where we'll store the fields of this relation
     */
    private final FrameDescriptorPart sourceFrame;

    private RowSink then;

    public RelParquet(URI file, MessageType schema, List<NamedProjection> project) {
        this.file = file;
        this.schema = schema;
        this.project = project;
        this.readers = new StatementFlatten[project.size()];
        this.sourceFrame = FrameDescriptorPart.root(project.size());

        // Check that we are only projecting primitive columns
        for (NamedProjection each : project) {
            assert schema.getType(each.projection.path).isPrimitive() : "Can't project group at " + each.projection + " " + schema.getType(each.projection.path);
        }

        // Allocate frame slots for each column
        Map<Projection, FrameSlot> slotsByPath = new HashMap<>();

        for (int i = 0; i < project.size(); i++) {
            FrameSlot slot = sourceFrame.findFrameSlot(i);
            Projection projection = project.get(i).projection;

            assert !slotsByPath.containsKey(projection) : "Cannot store projection " + projection + " twice";

            slotsByPath.put(projection, slot);
        }

        for (int i = 0; i < project.size(); i++) {
            StatementFlatten reader = StatementFlatten.compile(schema, project.get(i).projection, slotsByPath::get);

            readers[i] = reader;
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

            for (StatementFlatten each : readers) {
                each.prepare(readStore);
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
        for (StatementFlatten each : readers)
            each.read(frame);

        then.executeVoid(frame);

        for (StatementFlatten each : readers)
            each.consumeRepeats(frame);
    }

    @Override
    public void bind(LazyRowSink next) {
        then = next.apply(sourceFrame);

        // TODO does this create a copy of the AST under each node of the flatten tree?
        for (StatementFlatten each : readers) {
            each.bind(then);
        }
    }
}
