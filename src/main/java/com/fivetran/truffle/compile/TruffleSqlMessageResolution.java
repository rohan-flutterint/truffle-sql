package com.fivetran.truffle.compile;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.interop.CanResolve;
import com.oracle.truffle.api.interop.MessageResolution;
import com.oracle.truffle.api.interop.Resolve;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.nodes.Node;

import java.time.Instant;
import java.time.LocalDate;

/**
 * Based on SLFunctionMessageResolution
 */
@MessageResolution(receiverType = TruffleSqlFunction.class, language = TruffleSqlLanguage.class)
class TruffleSqlMessageResolution {
    /*
     * An SL function resolves an EXECUTE message.
     */
    @Resolve(message = "EXECUTE")
    public abstract static class SLForeignFunctionExecuteNode extends Node {

        public Object access(VirtualFrame frame, TruffleSqlFunction receiver, Object[] arguments) {
            Object[] arr = new Object[arguments.length];
            // Before the arguments can be used by the SLFunction, they need to be converted to SL
            // values.
            for (int i = 0; i < arr.length; i++)
                arr[i] = fromForeignValue(arguments[i]);

            return receiver.callTarget.call(arr);
        }
    }

    public static Object fromForeignValue(Object a) {
        // Types in SqlTypes
        // boolean.class, long.class, double.class, LocalDate.class, Instant.class, String.class, SqlNull.class
        if (a instanceof Boolean || a instanceof Long || a instanceof Double || a instanceof LocalDate || a instanceof Instant || a instanceof String)
            return a;
        else if (a instanceof TruffleObject)
            return a;
        else if (a instanceof TruffleSqlContext)
            return a;

        CompilerDirectives.transferToInterpreter();

        throw new IllegalStateException(a + " is not a Truffle value");
    }

    /*
     * An SL function should respond to an IS_EXECUTABLE message with true.
     */
    @Resolve(message = "IS_EXECUTABLE")
    public abstract static class SLForeignIsExecutableNode extends Node {
        public Object access(Object receiver) {
            return receiver instanceof TruffleSqlFunction;
        }
    }

    @CanResolve
    public abstract static class CheckFunction extends Node {

        protected static boolean test(TruffleObject receiver) {
            return receiver instanceof TruffleSqlFunction;
        }
    }
}
