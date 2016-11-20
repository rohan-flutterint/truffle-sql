package com.fivetran.truffle.compile;

import com.fivetran.truffle.parse.PhysicalRel;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.MaterializedFrame;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.source.Source;
import com.oracle.truffle.api.source.SourceSection;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;

import java.io.IOException;
import java.util.function.Consumer;

@TruffleLanguage.Registration(name = "SQL", version = "0.1", mimeType = TruffleSqlLanguage.MIME_TYPE)
public class TruffleSqlLanguage extends TruffleLanguage<TruffleSqlContext> {
    public static final String MIME_TYPE = "application/x-sql";

    /**
     * The singleton instance of the language.
     */
    public static final TruffleSqlLanguage INSTANCE = new TruffleSqlLanguage();

    private TruffleSqlLanguage() { }

    @Override
    protected TruffleSqlContext createContext(Env env) {
        return TruffleSqlContext.fromEnv(env);
    }

    @Override
    public CallTarget parse(Source source, Node context, String... strings) {
        // We don't invoke Truffle-SQL through the normal mechanism
        throw new UnsupportedOperationException();

    }

    @Override
    protected Object findExportedSymbol(TruffleSqlContext context, String globalName, boolean onlyExplicit) {
        return null;
    }

    @Override
    protected Object getLanguageGlobal(TruffleSqlContext context) {
        // The context itself is the global function registry. SQL does not have global variables.
        return context;
    }

    @Override
    protected boolean isObjectOfLanguage(Object object) {
        // Not sure what this does
        return false;
    }

    @Override
    protected Object evalInContext(Source source, Node node, MaterializedFrame mFrame) throws IOException {
        throw new IllegalStateException("evalInContext not supported in SQL");
    }

    /**
     * Main entry point of compiler.
     * Given a physical query plan from Calcite, produce an executable program.
     *
     * This implementation is optimized for sending `Object[]` results back to the user;
     * query plans that produce intermediate results may want to call compile(RelRoot, LazyRowSink) for better performance.
     */
    public CallTarget compileInteractiveQuery(RelRoot plan, Consumer<Object[]> then) {
        LazyRowSink sink = resultFrame -> new RowSink() {
            @Override
            public void bind(LazyRowSink next) {
                throw new UnsupportedOperationException("Final stage cannot be used as a source");
            }

            @Override
            public void executeVoid(VirtualFrame frame) {
                Object[] values = new Object[resultFrame.size()];

                for (int i = 0; i < resultFrame.size(); i++) {
                    FrameSlot slot = resultFrame.findFrameSlot(i);
                    Object truffleValue = frame.getValue(slot);
                    RelDataType type = plan.validatedRowType.getFieldList().get(i).getType();
                    Object resultSetValue = com.fivetran.truffle.Types.resultSet(truffleValue, type);

                    values[i] = resultSetValue;
                }

                then.accept(values);
            }
        };

        return compile(plan, sink);
    }

    private CallTarget compile(RelRoot plan, LazyRowSink sink) {
        // Compile query into Truffle program
        PhysicalRel physical = (PhysicalRel) plan.rel;
        RowSource compiled = physical.compile();

        compiled.bind(sink);

        // Make executable
        SourceSection sourceSection = SourceSection.createUnavailable("?", "Compiled query");
        SqlRootNode root = new SqlRootNode(sourceSection, compiled);

        return Truffle.getRuntime().createCallTarget(root);
    }
}
