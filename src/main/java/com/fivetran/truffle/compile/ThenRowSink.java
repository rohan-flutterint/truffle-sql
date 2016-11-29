package com.fivetran.truffle.compile;

/**
 * An almost-compiled relational expression.
 *
 * Calcite represents relation expressions in "pull" form, transform -> source.
 * We represent in "push" form, source -> transform.
 * The compile method {@link com.fivetran.truffle.parse.PhysicalRel#compile(ThenRowSink)}
 * takes a ThenRowSink callback, which allows us to "flip" the tree.
 */
@FunctionalInterface
public interface ThenRowSink {
    RowSink apply(FrameDescriptorPart frame);
}
