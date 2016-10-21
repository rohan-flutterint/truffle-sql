package com.fivetran.truffle;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.ExecutionContext;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.MaterializedFrame;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.Source;
import com.oracle.truffle.api.source.SourceSection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

@TruffleLanguage.Registration(name = "SL", version = "0.1", mimeType = TruffleSqlLanguage.MIME_TYPE)
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
    protected CallTarget parse(Source source, Node node, String... strings) throws IOException {
        return Truffle.getRuntime().createCallTarget(new RootNode(TruffleSqlLanguage.class, SourceSection.createUnavailable("Fake", "main"), new FrameDescriptor()) {
            @Override
            public Object execute(VirtualFrame frame) {
                TruffleSqlContext context = (TruffleSqlContext) frame.getArguments()[0];

                context.out.println("Hello world!");

                return null;
            }
        });
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
}
