package com.fivetran.truffle.compile;

import com.fivetran.truffle.Parquets;
import com.oracle.truffle.api.CompilerDirectives.TruffleBoundary;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.object.DynamicObject;
import com.oracle.truffle.api.object.Shape;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

class ExprAssembleGroup extends ExprAssemble {
    /**
     * Type that we are assembling
     */
    private final GroupType type;

    /**
     * Type of each column that contributes to the group we are assembling
     */
    private final ColumnDescriptor[] columns;

    /**
     * Readers for each column, in the block we are currently reading.
     * Initially these are all null.
     * They are set by prepare(ColumnReadStore) before we call execute()
     */
    private final ColumnReader[] readers;

    /**
     * Shape of the DynamicObject we will produce.
     * Initially this is set optimistically assuming values are never null.
     * When we observe nulls, we change the types of those fields.
     */
    private Shape shape;

    ExprAssembleGroup(MessageType root, String[] path) {
        this.type = (GroupType) root.getType(path);
        this.columns = root.getColumns()
                .stream()
                .filter(c -> Parquets.containsPath(path, c.getPath()))
                .toArray(ColumnDescriptor[]::new);
        this.readers = new ColumnReader[columns.length];
        this.shape = Parquets.shapeOptimistic(this.type);
    }

    @Override
    @TruffleBoundary
    void prepare(ColumnReadStore readStore) {
        for (int i = 0; i < columns.length; i++)
            readers[i] = readStore.getColumnReader(columns[i]);
    }

    @Override
    @TruffleBoundary
    long getTotalValueCount() {
        long max = 0;

        for (ColumnReader reader : readers) {
            if (reader.getTotalValueCount() > max)
                max = reader.getTotalValueCount();
        }

        return max;
    }

    DynamicObject executeGeneric(VirtualFrame frame) {
        throw new UnsupportedOperationException(); // TODO
    }

    Shape defineProperty(Shape oldShape, Object name, Object value) {
        assert shape.isRelated(oldShape);

        shape = shape.defineProperty(name, value, 0);

        return shape;
    }
}
