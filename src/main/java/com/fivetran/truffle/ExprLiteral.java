package com.fivetran.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;

public abstract class ExprLiteral extends ExprBase {
    static ExprLiteral Boolean(boolean value) {
        return new ExprLiteral() {
            @Override
            public Object executeGeneric(VirtualFrame frame) {
                return value;
            }

        };
    }

    static ExprLiteral Long(long value) {
        return new ExprLiteral() {
            @Override
            public Object executeGeneric(VirtualFrame frame) {
                return value;
            }

        };
    }

    static ExprLiteral Double(double value) {
        return new ExprLiteral() {
            @Override
            public Object executeGeneric(VirtualFrame frame) {
                return value;
            }

        };
    }

    static ExprLiteral Object(Object value) {
        return new ExprLiteral() {
            @Override
            public Object executeGeneric(VirtualFrame frame) {
                return value;
            }

        };
    }
}
