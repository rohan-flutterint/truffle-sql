package com.fivetran.truffle.compile;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ForeignAccess {
    @Test
    public void echo() {
        ExprReadArgument functionBody = new ExprReadArgument(0);
        Echo echo = TruffleSqlLanguage.compileFunction(functionBody, Echo.class);
        String argument = "Hello!";
        String reply = echo.echo(argument);

        assertThat(reply, equalTo(argument));
    }
}

@FunctionalInterface
interface Echo {
    <T> T echo(T argument);
}