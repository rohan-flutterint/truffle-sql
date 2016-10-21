package com.fivetran.truffle;

import com.oracle.truffle.api.ExecutionContext;
import com.oracle.truffle.api.TruffleLanguage;

import java.io.*;

public class TruffleSqlContext extends ExecutionContext {
    public final BufferedReader in;
    public final PrintWriter out, err;

    public TruffleSqlContext(TruffleLanguage.Env env) {
        this.in = new BufferedReader(new InputStreamReader(env.in()));
        this.out = new PrintWriter(env.out());
        this.err = new PrintWriter(env.err());
    }

    public TruffleSqlContext(BufferedReader in, PrintWriter out, PrintWriter err) {
        this.in = in;
        this.out = out;
        this.err = err;
    }

    public TruffleSqlContext(InputStream in, OutputStream out, OutputStream err) {
        this.in = new BufferedReader(new InputStreamReader(in));
        this.out = new PrintWriter(out);
        this.err = new PrintWriter(err);
    }
}
