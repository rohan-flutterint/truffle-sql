package com.fivetran.truffle;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.source.MissingMIMETypeException;
import com.oracle.truffle.api.source.MissingNameException;
import com.oracle.truffle.api.source.Source;
import com.oracle.truffle.api.vm.PolyglotEngine;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

public class RunTest {

    @BeforeClass
    public static void printRuntime() {
        System.out.println("Running on " + Truffle.getRuntime().getName());
    }

    @Test
    public void helloWorld() throws MissingNameException, MissingMIMETypeException, IOException {
        ByteArrayInputStream in = new ByteArrayInputStream("".getBytes());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        PolyglotEngine engine = PolyglotEngine.newBuilder()
                .setIn(in)
                .setOut(out)
                .setErr(err)
                .build();
        Source source = Source.newBuilder("?")
                .mimeType(TruffleSqlLanguage.MIME_TYPE)
                .name("Main.sql")
                .build();
        TruffleSqlContext context = (TruffleSqlContext) engine.getLanguages()
                .get(TruffleSqlLanguage.MIME_TYPE)
                .getGlobalObject()
                .get();
        CallTarget main = TruffleSqlLanguage.INSTANCE.parse(source, null);

        main.call(context);

        context.out.flush();
        context.err.flush();

        assertThat(out.toString(), equalTo("Hello world!\n"));
    }
}
