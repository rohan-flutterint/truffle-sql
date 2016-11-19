package com.fivetran.truffle;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.MaterializedFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.source.Source;
import com.oracle.truffle.api.source.SourceSection;

import java.io.IOException;

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
    protected CallTarget parse(Source source, Node context, String... strings) {
        if (context == null || !(context instanceof ExprPlan))
            throw new IllegalArgumentException("Expected PlanPseudoNode but found " + context);

        ExprPlan plan = (ExprPlan) context;

        // Compile query into Truffle program
        RowSource compiled = CompileRel.compile(plan.plan.rel);

        compiled.bind(plan.then);

        SourceSection sourceSection = SourceSection.createUnavailable("?", "Compiled query");
        RelRoot root = new RelRoot(sourceSection, compiled);

        return Truffle.getRuntime().createCallTarget(root);
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
