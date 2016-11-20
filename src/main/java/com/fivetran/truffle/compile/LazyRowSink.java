package com.fivetran.truffle.compile;

/**
 * When we compile, we first compile a parent expression, then compile its child,
 * then link the child by calling {@link LateBind#bind(LazyRowSink)}.
 *
 * Sometimes we need to wait to actually compile the child until it is about to be linked to the parent.
 * At this point, we know the part of the frame that the parent is using to store its values.
 */
@FunctionalInterface
public interface LazyRowSink {
    RowSink apply(FrameDescriptorPart frame);
}
