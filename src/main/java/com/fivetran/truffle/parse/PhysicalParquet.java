package com.fivetran.truffle.parse;

import com.fivetran.truffle.NamedProjection;
import com.fivetran.truffle.Parquets;
import com.fivetran.truffle.Projection;
import com.fivetran.truffle.compile.RelParquet;
import com.fivetran.truffle.compile.RowSource;
import com.fivetran.truffle.compile.ThenRowSink;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Scans a parquet-format file.
 */
class PhysicalParquet extends TableScan implements PhysicalRel {

    /**
     * Location of the file. Could be a local file, S3.
     */
    final URI file;

    /**
     * The original schema of the file
     */
    final MessageType schema;

    /**
     * The paths we want to project from the file.
     * Might be changed by rules in RelOptPlanner to push down projections.
     */
    final List<NamedProjection> project;

    PhysicalParquet(RelOptCluster cluster,
                    RelTraitSet traitSet,
                    RelOptTable table,
                    URI file,
                    MessageType schema,
                    List<NamedProjection> project) {
        super(cluster, traitSet, table);

        assert getConvention() == PhysicalRel.CONVENTION;

        this.file = file;
        this.schema = schema;
        this.project = project;
    }

    static List<NamedProjection> projectAllPaths(MessageType schema) {
        return schema.getFields()
                .stream()
                .map(f -> new NamedProjection(f.getName(), Projection.of(f.getName())))
                .collect(Collectors.toList());
    }

    @Override
    public RelDataType deriveRowType() {
        RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        RelDataTypeFactory.FieldInfoBuilder acc = typeFactory.builder();

        for (NamedProjection each : project) {
            Type projectedType = schema.getType(each.projection.path);
            RelDataType sqlType = Parquets.sqlType(projectedType, typeFactory);

            acc.add(each.name, sqlType);
        }

        return acc.build();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = 0;
        double cpu = 0;
        double io = 0;

        for (Footer footer : Parquets.footers(file)) {
            for (BlockMetaData block : Parquets.blockMetaData(footer)) {
                rows += block.getRowCount();

                for (ColumnChunkMetaData column : block.getColumns()) {
                    if (schema.containsPath(column.getPath().toArray())) {
                        io += column.getTotalSize();
                        cpu += column.getTotalUncompressedSize();
                    }
                }
            }
        }

        return planner.getCostFactory().makeCost(rows, cpu, io);
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(RuleProjectParquet.INSTANCE);

        // TODO push down filters
    }

    @Override
    public RowSource compile(ThenRowSink next) {
        return RelParquet.compile(file, schema, project, next);
    }

    /**
     * Composes the existing project with an additional project.
     *
     * For example, if the existing project is x.y AS xy
     * and the new project is xy.z AS xyz
     * then the composition is x.y.z AS xyz
     */
    public PhysicalParquet withProject(List<NamedProjection> project) {
        // TODO check project <: schema
        PhysicalParquet result = new PhysicalParquet(getCluster(), traitSet, table, file, schema, compose(this.project, project));

        result.rowType = result.deriveRowType();

        return result;
    }

    private List<NamedProjection> compose(List<NamedProjection> first, List<NamedProjection> second) {
        Map<String, Projection> firstByName = first.stream().collect(Collectors.toMap(p -> p.name, f -> f.projection));

        return second.stream().map(p -> {
            Projection head = firstByName.get(p.projection.path[0]);

            assert head != null : "Referenced column " + p.projection.path[0] + " is not in existing projection " + project;

            Projection tail = p.projection.drop(1);
            Projection both = head.concat(tail);

            return new NamedProjection(p.name, both);
        }).collect(Collectors.toList());
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);

        for (NamedProjection each : project) {
            pw.item(each.name, each.projection);
        }

        return pw;
    }
}
