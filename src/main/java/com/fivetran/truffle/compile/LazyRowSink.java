package com.fivetran.truffle.compile;

@FunctionalInterface
public interface LazyRowSink {
    RowSink apply(FrameDescriptorPart frame);
}
