package com.fivetran.truffle;

@FunctionalInterface
public interface LazyRowSink {
    RowSink apply(FrameDescriptorPart frame);
}
