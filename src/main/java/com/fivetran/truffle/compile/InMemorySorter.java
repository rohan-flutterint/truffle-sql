package com.fivetran.truffle.compile;

import com.oracle.truffle.api.object.DynamicObject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class InMemorySorter implements TupleSorter {
    private final List<DynamicObject> buffer = new ArrayList<>();

    @Override
    public void add(DynamicObject value) {
        buffer.add(value);
    }

    @Override
    public void sort(Comparator<DynamicObject> comparator) {
        buffer.sort(comparator);
    }

    @Override
    public Iterator<DynamicObject> iterator() {
        return buffer.iterator();
    }
}
