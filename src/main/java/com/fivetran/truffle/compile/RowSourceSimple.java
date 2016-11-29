package com.fivetran.truffle.compile;

/**
 * Most RowSources send rows from 1 place, for example a file, to 1 place, for example a Project
 */
abstract class RowSourceSimple extends RowSource {
    protected final FrameDescriptorPart sourceFrame;

    /**
     * What to do with each record
     */
    @Child
    protected RowSink then;

    protected RowSourceSimple(FrameDescriptorPart sourceFrame, RowSink then) {
        this.sourceFrame = sourceFrame;
        this.then = then;
    }
}
