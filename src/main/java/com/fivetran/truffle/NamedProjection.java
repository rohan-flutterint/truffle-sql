package com.fivetran.truffle;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NamedProjection that = (NamedProjection) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(projection, that.projection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, projection);
    }
}
