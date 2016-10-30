package com.fivetran.truffle;

import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;

abstract class ExprLiteral extends ExprBase {
    static ExprLiteral Boolean(boolean value) {
        return new ExprLiteral() {
            @Override
            Object execute(VirtualFrame frame) {
                return value;
            }

            @Override
            FrameSlotKind kind() {
                return FrameSlotKind.Boolean;
            }
        };
    }

    static ExprLiteral Byte(byte value) {
        return new ExprLiteral() {
            @Override
            Object execute(VirtualFrame frame) {
                return value;
            }

            @Override
            FrameSlotKind kind() {
                return FrameSlotKind.Byte;
            }
        };
    }

    static ExprLiteral Int(int value) {
        return new ExprLiteral() {
            @Override
            Object execute(VirtualFrame frame) {
                return value;
            }

            @Override
            FrameSlotKind kind() {
                return FrameSlotKind.Int;
            }
        };
    }

    static ExprLiteral Long(long value) {
        return new ExprLiteral() {
            @Override
            Object execute(VirtualFrame frame) {
                return value;
            }

            @Override
            FrameSlotKind kind() {
                return FrameSlotKind.Long;
            }
        };
    }

    static ExprLiteral Float(float value) {
        return new ExprLiteral() {
            @Override
            Object execute(VirtualFrame frame) {
                return value;
            }

            @Override
            FrameSlotKind kind() {
                return FrameSlotKind.Float;
            }
        };
    }

    static ExprLiteral Double(double value) {
        return new ExprLiteral() {
            @Override
            Object execute(VirtualFrame frame) {
                return value;
            }

            @Override
            FrameSlotKind kind() {
                return FrameSlotKind.Double;
            }
        };
    }

    static ExprLiteral Object(Object value) {
        return new ExprLiteral() {
            @Override
            Object execute(VirtualFrame frame) {
                return value;
            }

            @Override
            FrameSlotKind kind() {
                return FrameSlotKind.Object;
            }
        };
    }
}
