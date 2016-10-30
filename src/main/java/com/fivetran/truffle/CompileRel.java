package com.fivetran.truffle;

import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.SourceSection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Objects;

/**
 * Compiles a RelNode into a Truffle syntax tree
 * Suppose we have a query like:
 *
 * {@code
 * SELECT id || ', ' || attr
 * FROM 's3://some-file.parquet'
 * }
 *
 * We want to emit code like:
 *
 * {@code
 * function main() {
 *     file = open('s3://some-file.parquet')
 *
 *     while (file.next())
 *       int id = file.getInt('id')
 *       Object attr = file.getAny('attr')
 *       emit(id + ', ' + attr) // .. send to user, save to s3, or whatever we want to do with the query
 * }
 * }
 */
public class CompileRel implements RelShuttle {
    // TODO actually compile

    // Used to sneakily return the result to
    private RowSource compiled;
    private final RootNode then;

    public static RowSource compile(RelNode rel, RootNode then) {
        CompileRel compiler = new CompileRel(then);

        rel.accept(compiler);

        Objects.requireNonNull(compiler.compiled, "Compiler did not produce any output");

        return compiler.compiled;
    }

    // Force using compile
    private CompileRel(RootNode then) {
        this.then = then;
    }

    @Override
    public RelNode visit(TableScan scan) {
        throw new UnsupportedOperationException("TableScan should be wrapped in LogicalProject");
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        throw new UnsupportedOperationException("TableFunctionScan");
    }

    @Override
    public RelNode visit(LogicalValues values) {
        compiled = new RelLiteral(SourceSection.createUnavailable("SQL query", "Literal"), values, then);

        return values;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalProject project) {
        // Multiple inputs should never occur in LogicalProject, only in other RelNode implementers like MultiJoin
        assert project.getInputs().size() <= 1 : "LogicalProject has " + project.getInputs().size() + " inputs";

        RelProject select = new RelProject(SourceSection.createUnavailable("SQL query", "Project"), project, then);

        if (project.getInputs().isEmpty())
            compiled = new RelEmpty(SourceSection.createUnavailable("SQL query", "Empty"), select);
        else
            compiled = compile(project.getInputs().get(0), select);

        return project;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelNode visit(RelNode other) {
        throw new UnsupportedOperationException();
    }

    static FrameDescriptor frame(RelDataType rowType) {
        List<RelDataTypeField> fields = rowType.getFieldList();
        FrameDescriptor describe = new FrameDescriptor();

        for (int column = 0; column < fields.size(); column++) {
            SqlTypeName type = fields.get(column).getType().getSqlTypeName();
            FrameSlotKind kind = kind(type);
            FrameSlot slot = describe.addFrameSlot(column, kind);
        }

        return describe;
    }

    static FrameSlotKind kind(SqlTypeName type) {
        switch (type) {
            case BOOLEAN:
                return FrameSlotKind.Boolean;
            case TINYINT:
                return FrameSlotKind.Byte;
            case SMALLINT:
            case INTEGER:
                return FrameSlotKind.Int;
            case BIGINT:
                return FrameSlotKind.Long;
            case FLOAT:
                return FrameSlotKind.Float;
            case REAL:
            case DOUBLE:
                return FrameSlotKind.Double;
            default:
                return FrameSlotKind.Object;
        }
    }
}
