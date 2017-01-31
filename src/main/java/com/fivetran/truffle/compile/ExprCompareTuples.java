package com.fivetran.truffle.compile;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExplodeLoop;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.calcite.rel.RelFieldCollation;

import java.util.List;

class ExprCompareTuples extends ExprBase {

    @Children
    private final ExprCompare[] compareEach;

    ExprCompareTuples(ExprBase left, ExprBase right, List<RelFieldCollation> orderBy) {
        compareEach = new ExprCompare[orderBy.size()];

        for (int i = 0; i < orderBy.size(); i++) {
            RelFieldCollation collation = orderBy.get(i);
            ExprReadProperty readLeft = ExprReadPropertyNodeGen.create(Integer.toString(i), left);
            ExprReadProperty readRight = ExprReadPropertyNodeGen.create(Integer.toString(i), right);
            boolean nullsAreLargest = collation.nullDirection == collation.direction.defaultNullDirection();
            boolean ascending = isAscending(collation.direction);

            compareEach[i] = ExprCompareNodeGen.create(nullsAreLargest, ascending, readLeft, readRight);
        }
    }

    private boolean isAscending(RelFieldCollation.Direction direction) {
        switch (direction) {
            case ASCENDING:
            case STRICTLY_ASCENDING:
                return true;
            case DESCENDING:
            case STRICTLY_DESCENDING:
                return false;
            default:
                throw new RuntimeException("Expected ascending or descending order but found " + direction);
        }
    }

    @ExplodeLoop
    @Override
    public Integer executeGeneric(VirtualFrame frame) {
        for (ExprCompare each : compareEach) {
            try {
                int compare = (int) each.executeLong(frame);

                if (compare != 0)
                    return compare;
            } catch (UnexpectedResultException e) {
                throw new RuntimeException(e);
            }
        }

        return 0;
    }
}
