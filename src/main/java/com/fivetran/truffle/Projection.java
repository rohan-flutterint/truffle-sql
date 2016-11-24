package com.fivetran.truffle;

import com.google.common.base.Joiner;

/**
 * Represents a projection from a nested schema, for example SELECT a.b
 *
 * Does not have to be a leaf node - for example, the schema could be document { a { b { c INT64 }}}
 * and we could project SELECT a.b
 */
public class Projection {
    public final String[] path;

    private Projection(String[] path) {
        this.path = path;
    }

    public static Projection of(String... path) {
        return new Projection(path);
    }

    public Projection prepend(String head) {
        return new Projection(prepend(head, path));
    }

    public Projection append(String tail) {
        return new Projection(append(path, tail));
    }

    private static String[] prepend(String head, String[] tail) {
        String[] result = new String[tail.length + 1];

        result[0] = head;

        System.arraycopy(tail, 0, result, 1, tail.length);

        return result;
    }

    private static String[] append(String[] head, String name) {
        String[] result = new String[head.length + 1];

        System.arraycopy(head, 0, result, 0, head.length);

        result[result.length - 1] = name;

        return result;
    }

    @Override
    public String toString() {
        return Joiner.on('.').join(path);
    }

    public Projection drop(int first) {
        String[] result = new String[path.length - first];

        System.arraycopy(path, first, result, 0, result.length);

        return new Projection(result);
    }

    public Projection concat(Projection tail) {
        String[] result = new String[path.length + tail.path.length];

        System.arraycopy(path, 0, result, 0, path.length);
        System.arraycopy(tail.path, 0, result, path.length, tail.path.length);

        return new Projection(result);
    }
}
