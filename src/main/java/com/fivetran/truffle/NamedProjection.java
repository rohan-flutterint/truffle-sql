package com.fivetran.truffle;

public class NamedProjection {
    public final String name;
    public final Projection projection;

    public NamedProjection(String name, Projection projection) {
        this.name = name;
        this.projection = projection;
    }

    @Override
    public String toString() {
        return projection + " AS " + name;
    }
}
