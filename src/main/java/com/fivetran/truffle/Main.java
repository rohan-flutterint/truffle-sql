package com.fivetran.truffle;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.source.Source;
import com.oracle.truffle.api.vm.PolyglotEngine;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class Main {
    public static void main(String[] args) throws IOException {
        if (args.length == 0)
            System.err.println("Usage: tsql Source.sql");
        else {
            System.err.println("Running on " + Truffle.getRuntime().getName());

            Path path = Paths.get(args[0]);
            Source source = Source.newBuilder(path.toFile())
                    .mimeType(TruffleSqlLanguage.MIME_TYPE)
                    .name(path.getFileName().toString())
                    .build();
            CallTarget main = TruffleSqlLanguage.INSTANCE.parse(source, null);

            callWithRootContext(main);
        }
    }

    public static void callWithRootContext(CallTarget main) {
        try {
            Objects.requireNonNull(main, "Program is null");

            PolyglotEngine engine = PolyglotEngine.newBuilder()
                    .setIn(System.in)
                    .setOut(System.out)
                    .setErr(System.err)
                    .build();
            TruffleSqlContext context = (TruffleSqlContext) engine.getLanguages()
                    .get(TruffleSqlLanguage.MIME_TYPE)
                    .getGlobalObject()
                    .get();

            main.call(context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
