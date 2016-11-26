package com.fivetran.truffle.compile;

import com.fivetran.truffle.Projection;
import com.oracle.truffle.api.dsl.TypeSystemReference;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.schema.MessageType;

import java.util.function.Function;

@TypeSystemReference(SqlTypes.class)
@NodeInfo(description = "The abstract base node for all statements")
abstract class StatementFlatten extends Node {
    @Child
    protected RowSink then;

    static StatementFlatten compile(MessageType schema, Projection path, Function<Projection, FrameSlot> findFrameSlot) {
        if (schema.getType(path.path).isPrimitive())
            return new StatementFlattenColumn(schema, path, findFrameSlot.apply(path));
        else
            return new StatementFlattenGroup(schema, path, findFrameSlot);
    }

    /**
     * Get ColumnReaders from file
     */
    abstract void prepare(ColumnReadStore file);

    /**
     * Read all columns once
     */
    abstract void read(VirtualFrame frame);

    /**
     * Read repeats on all groups and columns until none remain
     */
    abstract void consumeRepeats(VirtualFrame frame);

    /**
     * Does every column in this group repeat at the target level?
     */
    abstract boolean repeats(int targetRepetitionLevel);

    abstract void bind(RowSink next);
}
