package com.fivetran.truffle.compile;

import com.oracle.truffle.api.object.DynamicObject;

import java.util.Comparator;

/**
 * Sort tuples (represented by DynamicObject) outside of Truffle.
 *
 * Might be an in-memory sort backed by an array.
 * We could implement spill-to-disk sort, but it's not clear that makes sense in a multitenant system;
 * it's simpler just to allocate more memory for a shorter time.
 */
public interface TupleSorter extends Iterable<DynamicObject> {
    void add(DynamicObject value);
    void sort(Comparator<DynamicObject> comparator);
}