package com.fivetran.truffle.compile;

import com.oracle.truffle.api.CompilerAsserts;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.NodeChildren;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.object.*;

/**
 * Writes a field of a record we are assembling.
 * We know the shape of the record up-front,
 * but we would like to optimistically try to use primitive fields for nullable values.
 *
 * Based on SLWritePropertyNode and SLWritePropertyCacheNode
 */
@NodeChildren({
        @NodeChild(value = "receiverNode", type = ExprBase.class),
        @NodeChild(value = "valueNode", type = ExprBase.class)
})
abstract class StatementWriteProperty extends StatementBase {
    protected static final int CACHE_LIMIT = 3;

    /**
     * Name of the field we are writing.
     */
    protected String name;

    StatementWriteProperty(String name) {
        this.name = name;
    }

    abstract void executeWrite(VirtualFrame frame, Object receiver, Object value);

    /**
     * Polymorphic inline cache for writing a property that already exists (no shape change is necessary)
     */
    @Specialization(
            limit = "CACHE_LIMIT", //
            guards = {
                    "shapeCheck(shape, receiver)",
                    "location != null",
                    "canSet(location, value)"
            }, //
            assumptions = {
                    "shape.getValidAssumption()"
            })
    protected static void writeExistingPropertyCached(DynamicObject receiver,
                                                      Object value,
                                                      @Cached("lookupShape(receiver)") Shape shape,
                                                      @Cached("lookupLocation(shape, name, value)") Location location) {
        try {
            location.set(receiver, value, shape);
        } catch (IncompatibleLocationException | FinalLocationException ex) {
            /* Our guards ensure that the value can be stored, so this cannot happen. */
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Polymorphic inline cache for writing a property that does not exist yet (shape change is necessary)
     */
    @Specialization(
            limit = "CACHE_LIMIT",
            guards = {
                    "shapeCheck(oldShape, receiver)",
                    "oldLocation == null",
                    "canStore(newLocation, value)"
            },
            assumptions = {
                    "oldShape.getValidAssumption()",
                    "newShape.getValidAssumption()"
            })
    protected static void writeNewPropertyCached(DynamicObject receiver,
                                                 Object value,
                                                 @Cached("lookupShape(receiver)") Shape oldShape,
                                                 @Cached("lookupLocation(oldShape, name, value)") Location oldLocation,
                                                 @Cached("defineProperty(oldShape, name, value)") Shape newShape,
                                                 @Cached("lookupLocation(newShape, name)") Location newLocation) {
        try {
            newLocation.set(receiver, value, oldShape, newShape);

        } catch (IncompatibleLocationException ex) {
            /* Our guards ensure that the value can be stored, so this cannot happen. */
            throw new IllegalStateException(ex);
        }
    }

    protected static boolean shapeCheck(Shape shape, DynamicObject receiver) {
        return shape != null && shape.check(receiver);
    }

    protected static Shape lookupShape(DynamicObject receiver) {
        CompilerAsserts.neverPartOfCompilation();

        if (!TruffleSqlContext.isSqlObject(receiver)) {
            /* The specialization doForeignObject handles this case. */
            return null;
        }
        return receiver.getShape();
    }

    /**
     * There is a subtle difference between {@link Location#canSet} and {@link Location#canStore}.
     * We need {@link Location#canSet} for the guard of {@link #writeExistingPropertyCached} because
     * there we call {@link Location#set}. We use the more relaxed {@link Location#canStore} for the
     * guard of writeNewPropertyCached because there we perform a
     * shape transition, i.e., we are not actually setting the value of the new location - we only
     * transition to this location as part of the shape change.
     */
    protected static boolean canSet(Location location, Object value) {
        return location.canSet(value);
    }

    /** See {@link #canSet} for the difference between the two methods. */
    protected static boolean canStore(Location location, Object value) {
        return location.canStore(value);
    }

    /** Try to find the given property in the shape. */
    protected static Location lookupLocation(Shape shape, Object name) {
        CompilerAsserts.neverPartOfCompilation();

        Property property = shape.getProperty(name);
        if (property == null) {
            /* Property does not exist yet, so a shape change is necessary. */
            return null;
        }

        return property.getLocation();
    }

    /**
     * Try to find the given property in the shape. Also returns null when the value cannot be store
     * into the location.
     */
    protected static Location lookupLocation(Shape shape, Object name, Object value) {
        Location location = lookupLocation(shape, name);
        if (location == null || !location.canSet(value)) {
            /* Existing property has an incompatible type, so a shape change is necessary. */
            return null;
        }

        return location;
    }

    protected static Shape defineProperty(Shape oldShape, Object name, Object value) {
        return oldShape.defineProperty(name, value, 0);
    }
}
