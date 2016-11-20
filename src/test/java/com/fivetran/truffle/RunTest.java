package com.fivetran.truffle;

import com.fivetran.truffle.compile.TruffleSqlContext;
import com.fivetran.truffle.compile.TruffleSqlLanguage;
import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.RootNode;
import com.oracle.truffle.api.source.MissingMIMETypeException;
import com.oracle.truffle.api.source.MissingNameException;
import com.oracle.truffle.api.source.SourceSection;
import com.oracle.truffle.api.vm.PolyglotEngine;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

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
        TruffleSqlContext context = (TruffleSqlContext) engine.getLanguages()
                .get(TruffleSqlLanguage.MIME_TYPE)
                .getGlobalObject()
                .get();
        CallTarget main = Truffle.getRuntime().createCallTarget(new RootNode(TruffleSqlLanguage.class, SourceSection.createUnavailable("?", "Test.sql"), new FrameDescriptor()) {
            @Override
            public Object execute(VirtualFrame frame) {
                TruffleSqlContext context = (TruffleSqlContext) frame.getArguments()[0];

                context.out.println("Hello world!");

                return null;
            }
        });

        main.call(context);

        context.out.flush();
        context.err.flush();

        assertThat(out.toString(), equalTo("Hello world!\n"));
    }
}
