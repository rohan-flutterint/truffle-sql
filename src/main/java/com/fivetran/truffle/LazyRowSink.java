package com.fivetran.truffle;

@FunctionalInterface
interface LazyRowSink {
    RowSink apply(FrameDescriptorPart frame);
}
