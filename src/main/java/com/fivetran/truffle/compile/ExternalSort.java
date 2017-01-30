package com.fivetran.truffle.compile;

import com.fivetran.truffle.parse.PhysicalRel;
import com.oracle.truffle.api.object.DynamicObject;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.ArrayList;
import java.util.List;
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
// TODO sort should probably be a local operation, with shuffle as the global operation
public class ExternalSort extends RowSource {

    /**
     * First stage flushes input relation to external sorter
     */
    @Child
    private RowSource inputToSort;

    /**
     * Second stage flushes externally sorted rows to output
     */
    @Child
    private RowSource sortToOutput;

    public ExternalSort(RowSource inputToSort, RowSource sortToOutput) {
        this.inputToSort = inputToSort;
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

        // This is just a placeholder for an external sorter
        // Doesn't actually sort and isn't external
        // TODO replace with a string reference to an external stage, which is effectively a global variable
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

        return new ExternalSort(inputToSort, sortToOutput);
    }

    @Override
    protected void executeVoid() {
        inputToSort.executeVoid();
        sortToOutput.executeVoid();
    }
}
