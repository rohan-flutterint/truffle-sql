package com.fivetran.truffle;

import java.util.Objects;

/**
 * A projections from a parquet file, and a new name for it.
 *
 * You would think that we could use {@code Map<String, Projection>} for this purpose and avoid needing this type.
 * However, the order of fields is important in Calcite, so we use {@code List<NamedProjection>}.
 */
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
