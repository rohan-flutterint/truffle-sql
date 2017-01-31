package com.fivetran.truffle.compile;

import com.fivetran.truffle.parse.PhysicalRel;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.object.DynamicObject;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

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

    private final List<DynamicObject> externalSorter;

    private final Comparator<DynamicObject> comparator;

    /**
     * Second stage flushes externally sorted rows to output
     */
    @Child
    private RowSource sortToOutput;

    public ExternalSort(RowSource inputToSort,
                        List<DynamicObject> externalSorter,
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
                                       ThenRowSink next) {
        if (orderBy.isEmpty())
            throw new IllegalArgumentException("orderBy should not be empty");

        List<DynamicObject> externalSorter = new ArrayList<>();

        // First stage sends rows to external sorter
        RowSource inputToSort = input.compile(sourceFrame -> {
            ExprMaterializeTuple materializeTuple = ExprMaterializeTuple.compile(sourceFrame);

            return new ExternalSortInput(materializeTuple, externalSorter::add);
        });

        // Second stage pulls rows from external sorter and sends them to next
        Supplier<DynamicObject> nextRow = new Supplier<DynamicObject>() {
            int i = 0;

            @Override
            public DynamicObject get() {
                if (i < externalSorter.size()) {
                    DynamicObject result = externalSorter.get(i);

                    i++;

                    return result;
                }
                else return null;
            }
        };
        ExternalSortOutput sortToOutput = ExternalSortOutput.compile(nextRow, input.getRowType(), next);

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
