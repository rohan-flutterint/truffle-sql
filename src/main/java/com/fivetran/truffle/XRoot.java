package com.fivetran.truffle;


import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.SourceSection;

public class XRoot extends RootNode {
    final XRel query;

    public XRoot(XRel query, Class<? extends TruffleLanguage> language, SourceSection sourceSection, FrameDescriptor frameDescriptor) {
        super(language, sourceSection, frameDescriptor);

        this.query = query;
    }

    @Override
    public Object execute(VirtualFrame frame) {
        TruffleSqlContext context = (TruffleSqlContext) frame.getArguments()[0];
        Sink sink = (Sink) frame.getArguments()[1];

        int nRows = (int) query.get(0).getTotalValueCount();

        for (int row = 0; row < nRows; row++) {
            Object[] tuple = new Object[query.size()];

            for (int column = 0; column < query.size(); column++) {
                XIterator it = query.get(column);

                // TODO actually implement repetition, definition levels
                assert it.getCurrentRepetitionLevel() == 0;
                assert it.getCurrentDefinitionLevel() == 0;
                assert !it.isFullyConsumed();

                Object value = it.getObject();

                // We are leaving the Truffle system, so we should convert NullValue to null
                if (value == NullValue.INSTANCE)
                    value = null;

                tuple[column] = value;
            }

            sink.accept(tuple);
        }

        return null;
    }
}
