package com.fivetran.truffle.compile;

import com.oracle.truffle.api.dsl.UnsupportedSpecializationException;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.Node;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import com.oracle.truffle.api.profiles.ConditionProfile;

/**
 * Based on SLIfNode
 */
class ExprIf extends ExprBase {
    @Child
    protected ExprTest conditionNode;
    @Child
    protected ExprBase thenPartNode, elsePartNode;

    ExprIf(ExprTest conditionNode, ExprBase thenPartNode, ExprBase elsePartNode) {
        this.conditionNode = conditionNode;
        this.thenPartNode = thenPartNode;
        this.elsePartNode = elsePartNode;
    }

    private final ConditionProfile condition = ConditionProfile.createCountingProfile();

    // TODO this is not specialized
    // either add a bunch of @Specialized implementations,
    // or refactor to write then / else parts to write to a local variable
    @Override
    public Object executeGeneric(VirtualFrame frame) {
        /*
         * In the interpreter, record profiling information that the condition was executed and with
         * which outcome.
         */
        if (condition.profile(evaluateCondition(frame)))
            return thenPartNode.executeGeneric(frame);
        else
            return elsePartNode.executeGeneric(frame);
    }

    private boolean evaluateCondition(VirtualFrame frame) {
        try {
            /*
             * The condition must evaluate to a boolean value, so we call the boolean-specialized
             * execute method.
             */
            return conditionNode.executeBoolean(frame);
        } catch (UnexpectedResultException ex) {
            /*
             * The condition evaluated to a non-boolean result. This is a type error in the SL
             * program. We report it with the same exception that Truffle DSL generated nodes use to
             * report type errors.
             */
            throw new UnsupportedSpecializationException(this, new Node[]{conditionNode}, ex.getResult());
        }
    }
}
