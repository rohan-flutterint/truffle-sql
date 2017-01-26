package com.fivetran.truffle.compile;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.DirectCallNode;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.object.Shape;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Implements GROUP BY queries, assuming that the data is already sorted by the GROUP BY key.
 *
 * If we are shuffling the data before performing GROUP BY (likely),
 * it is advantageous to fully sort the data by the partition key on the map side
 * (see http://blog.cloudera.com/blog/2015/01/improving-sort-performance-in-apache-spark-its-a-double/).
 * Therefore, we simply assume that the data will always be presorted by the GROUP BY key.
 *
 * We start with an incomplete program that generates rows (the input),
 * and an incomplete program that processes aggregated rows (the output):
 *
 * # Input
 * for (row in input)
 *   ?do something with input?
 *
 * # Output
 * doSomething(?result of aggregate?)
 *
 * Our strategy is to initialize our aggregator
 *
 * sum = null
 * groupBy = null
 * started = false
 * for (row in input)
 *   if (!started)
 *     sum = row.sum
 *     groupBy = row.groupBy
 *     started = true
 *   else if (row.groupBy == groupBy)
 *     sum += row.sum
 *   else
 *     doSomething(sum)
 *     sum = row.sum
 *     groupBy = row.groupBy
 * doSomething(sum)
 */
public class RelAggregate extends RowSource {

    /**
     * Top of the frame we'll use during this stage.
     *
     * This will hold:
     *   boolean started
     *   ? groupByColumn...
     *   ? aggregateColumn...
     */
    private final FrameDescriptorPart sourceFrame;

    /**
     * Shape of the aggregator object.
     *
     * The aggregator object represents the current, incrementally-calculated aggregate.
     * For example, if we are calculating SELECT sum(n), count() FROM t
     * the object would have shape { long sum, long count }.
     */
    private final Shape aggregator;

    /**
     * Initializes aggregate object; sends values to sink.
     */
    @Child
    private final RowSink source;

    /**
     * Where to send aggregated rows
     */
    @Child
    private final DirectCallNode sink;

    public static RelAggregate compile(ThenRowSink input,
                                       ThenRowSink output,
                                       ImmutableBitSet group,
                                       List<AggregateCall> aggregate) {
    }

    @Override
    protected void executeVoid() {
        VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(new Object[]{}, sourceFrame.frame());

        source.executeVoid(frame);

        // Flush the last row
        then.executeVoid(frame);
    }
}

/**
 * Given an incoming row, update incremental aggregators.
 */
class AggregateSorted extends ExprBase {

    private final FrameDescriptorPart sourceFrame;

    /**
     * Represents the current value of each column in GROUP BY
     */
    private final FrameDescriptorPart group;

    /**
     * Represents the current value of each aggregated column
     */
    private final FrameDescriptorPart aggregate;

    /**
     * Transfers the values of the GROUP BY columns from sourceFrame to group
     */
    @Children
    private final StatementWriteLocal[] setGroup;

    /**
     * Reset the values of each column in aggregate to the zero value
     */
    @Children
    private final StatementWriteLocal[] resetAggregate;

    /**
     * Add the current values from sourceFrame into aggregate
     */
    @Children
    private final StatementWriteLocal[] incrementAggregate;

    @Override
    @ExplodeLoop
    public boolean executeBoolean(VirtualFrame frame) {
        // If GROUP BY columns have changed, we are in a new partition
        if (groupChanged()) {
            // Set the new values of the GROUP BY columns
            for (StatementWriteLocal each : setGroup)
                each.executeVoid(frame);

            // Reset the values of the aggregate columns
            for (StatementWriteLocal each : resetAggregate)
                each.executeVoid(frame);
        }
        else {
            for (StatementWriteLocal each : incrementAggregate) {
                each.executeVoid(frame);
            }
        }
    }

    /**
     * Do the current values in group match the values in sourceFrame?
     * If so, we are in a new partition, and we should push out a row and reset the aggregates.
     */
    private boolean groupChanged() {

    }

    /**
     * Copy corresponding columns from sourceFrame to group
     */
    private void copySourceFrameToGroup() {

    }
}
