package com.fivetran.truffle.compile;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.object.DynamicObject;

import java.util.function.Consumer;

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
