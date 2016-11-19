package com.fivetran.truffle;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.TypeVisitor;

import java.util.Objects;

/**
 * Compiles a parquet projection (MessageType) into a statement (StatementBase)
 * that assembles 1 or more parquet columns (value, repetition level, definition level)
 * into 1 column (VirtualFrame slot) ready for querying.
 *
 * In the simplest case, the MessageType will be a simple primitive value,
 * and the compiled RowSource will simply decompress it and stick it in a primitive VirtualFrame slot.
 *
 * In general, Parquet columns can be arbitrary nested schemas,
 * in which case we will construct a finite-state-machine as described in the Dremel paper:
 * http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/36632.pdf.
 */
class CompileParquet implements TypeVisitor {
    public static ExprAssemble compile(MessageType parent, String[] path) {
        CompileParquet compiler = new CompileParquet(parent, path);

        parent.getType(path).accept(compiler);

        Objects.requireNonNull(compiler.result, "CompileParquet did not produce any output");

        return compiler.result;
    }

    /**
     * Message type of the entire Parquet file.
     * The file might have additional structure that nobody is projecting, but this is not our concern here.
     */
    private final MessageType root;

    /**
     * Path into parent that we want to project into a single column.
     */
    private final String[] path;

    private ExprAssemble result;

    CompileParquet(MessageType root, String[] path) {
        this.root = root;
        this.path = path;
    }

    @Override
    public void visit(GroupType type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(MessageType type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(PrimitiveType type) {
        result = ExprAssembleColumnNodeGen.create(root, path);
    }
}
