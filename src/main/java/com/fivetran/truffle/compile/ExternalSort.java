package com.fivetran.truffle.compile;

import com.fivetran.truffle.parse.PhysicalRel;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.object.DynamicObject;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

/**
 * External-sort is a two-stage operation:
 *
 * sort = newExternalSortStage()
 * input -> sort
 * sort -> output
 *
 * `input` and `output` are two separate RowSource programs,
 * which each have a reference to the same external sort stage,
 * which is a kind of global variable.
 */
public class ExternalSort extends RowSource {

    /**
     * First stage flushes input relation to external sorter
     */
    @Child
    private RowSource inputToSort;

    private final TupleSorter externalSorter;

    private final Comparator<DynamicObject> comparator;

    /**
     * Second stage flushes externally sorted rows to output
     */
    @Child
    private RowSource sortToOutput;

    public ExternalSort(RowSource inputToSort,
                        TupleSorter externalSorter,
                        Comparator<DynamicObject> comparator,
                        RowSource sortToOutput) {
        this.inputToSort = inputToSort;
        this.externalSorter = externalSorter;
        this.comparator = comparator;
        this.sortToOutput = sortToOutput;
    }

    /**
     * Compile a two-stage program like:
     *
     *
     * @param input Relation that we want to sort
     * @param orderBy Slots of sourceFrame that we will order by
     * @param next What to do with the sorted data
     * @return Input and output sides of external-sort, with the actual sort operation as a black-box in between
     */
    public static ExternalSort compile(PhysicalRel input,
                                       List<RelFieldCollation> orderBy,
                                       TupleSorter externalSorter,
                                       ThenRowSink next) {
        if (orderBy.isEmpty())
            throw new IllegalArgumentException("orderBy should not be empty");

        // First stage sends rows to external sorter
        RowSource inputToSort = input.compile(sourceFrame -> {
            ExprMaterializeTuple materializeTuple = ExprMaterializeTuple.compile(sourceFrame);

            return new ExternalSortInput(materializeTuple, externalSorter::add);
        });

        // Second stage pulls rows from external sorter and sends them to next
        ExternalSortOutput sortToOutput = ExternalSortOutput.compile(externalSorter, input.getRowType(), next);

        // Compile a custom function that can be called from Java that compares tuples
        Comparator<DynamicObject> compareFn = compileComparator(orderBy);

        return new ExternalSort(inputToSort, externalSorter, compareFn, sortToOutput);
    }

    @SuppressWarnings("unchecked")
    private static Comparator<DynamicObject> compileComparator(List<RelFieldCollation> orderBy) {
        ExprReadArgument left = new ExprReadArgument(0), right = new ExprReadArgument(1);
        ExprCompareTuples compareExpression = new ExprCompareTuples(left, right, orderBy);
        Class<Comparator> asType = Comparator.class;
        Comparator comparator = TruffleSqlLanguage.compileFunction(compareExpression, asType);

        return (Comparator<DynamicObject>) comparator;
    }

    @Override
    protected void executeVoid() {
        inputToSort.executeVoid();

        externalSorter.sort(comparator);

        sortToOutput.executeVoid();
    }
}

class ExternalSortInput extends RowSink {
    /**
     * Materializes each row
     */
    @Child
    private ExprMaterializeTuple materializeTuple;

    /**
     * Externally sorts somewhere.
     *
     * It's kind of weird that we have a direct reference to the external sorter.
     * The reason this works is because each Truffle program is compiled and run only once.
     */
    private final Consumer<DynamicObject> sorter;

    ExternalSortInput(ExprMaterializeTuple materializeTuple, Consumer<DynamicObject> sorter) {
        this.materializeTuple = materializeTuple;
        this.sorter = sorter;
    }

    @Override
    public void executeVoid(VirtualFrame frame) {
        DynamicObject row = (DynamicObject) materializeTuple.executeGeneric(frame);

        sorter.accept(row);
    }
}

class ExternalSortOutput extends RowSource {
    /**
     * External sorter that produces the next row from the external sort.
     *
     * It's kind of weird that we have a direct reference to the external sorter.
     * The reason this works is because each Truffle program is compiled and run only once.
     */
    private final Iterable<DynamicObject> sorter;

    /**
     * Shape of frame we will allocate for this stage
     */
    private final FrameDescriptor sourceFrame;

    /**
     * Slot where we will put the result of sorter.next()
     */
    private final FrameSlot tupleSlot;

    /**
     * Write all columns of tuple from tupleSlot into VirtualFrame
     */
    @Child
    private StatementExplodeTuple explodeTuple;

    /**
     * What to do with each row from external sort
     */
    @Child
    private RowSink then;

    private ExternalSortOutput(Iterable<DynamicObject> sorter,
                               FrameDescriptor sourceFrame,
                               FrameSlot tupleSlot,
                               StatementExplodeTuple explodeTuple,
                               RowSink then) {
        this.sorter = sorter;
        this.sourceFrame = sourceFrame;
        this.tupleSlot = tupleSlot;
        this.explodeTuple = explodeTuple;
        this.then = then;
    }

    /**
     * @param sorter External sorter that will feed input to this stage
     * @param inputShape Shape of rows we created on the input side, which should indicate D
     * @param next What to do with each row in this stage
     */
    static ExternalSortOutput compile(Iterable<DynamicObject> sorter,
                                      RelDataType inputShape,
                                      ThenRowSink next) {
        // 1 slot for the DynamicObject tuple, n frames for the n exploded columns
        FrameDescriptorPart tupleFrame = FrameDescriptorPart.root(1);
        FrameDescriptorPart columnsFrame = tupleFrame.push(inputShape.getFieldCount());
        StatementExplodeTuple explodeTuple = StatementExplodeTuple.compile(tupleFrame.findFrameSlot(0), columnsFrame);
        RowSink then = next.apply(columnsFrame);

        return new ExternalSortOutput(sorter, tupleFrame.frame(), tupleFrame.findFrameSlot(0), explodeTuple, then);
    }

    @Override
    protected void executeVoid() {
        VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(new Object[]{}, sourceFrame);

        for (DynamicObject tuple : sorter) {
            // tuple = sorter.get()
            frame.setObject(tupleSlot, tuple);

            // a = tuple[0]
            // b = tuple[1]
            // ...
            explodeTuple.executeVoid(frame);

            // ...execute rest of stage...
            then.executeVoid(frame);
        }
    }
}
