package com.fivetran.truffle.parse;

import com.fivetran.truffle.compile.TruffleSqlLanguage;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.oracle.truffle.api.CallTarget;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Util;

import java.lang.reflect.Type;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Main entry point for Calcite. Teaches calcite how to connect to jdbc:truffle//... URIs
 */
public class TruffleMeta extends MetaImpl {

    /**
     * More forgiving versions of standard operators,
     * plus anything registered using {@link TruffleMeta#registerMacro(String, TableMacro)}
     */
    private static final ReflectiveSqlOperatorTable CUSTOM_OPERATORS = ForgivingOperatorTable.instance();

    private static final SqlOperatorTable OPERATORS = createOperatorTable();

    public TruffleMeta(AvaticaConnection connection) {
        super(connection);
    }

    @Override
    public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
        StatementHandle statement = createStatement(ch);

        statement.signature = validate(sql, parse(sql));

        return statement;
    }

    public static SqlNode parse(String sql) {
        try {
            SqlParser.Config config = SqlParser.configBuilder().setLex(Lex.JAVA).build();
            SqlParser parser = SqlParser.create(sql, config);

            return parser.parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static Signature validate(String query, SqlNode parsed) {
        SqlValidatorImpl validator = validator();

        // Validate the query
        // Has the side-effect of storing the RelDataType in an internal cache of validator
        validator.validate(parsed);

        // Type of query
        RelDataType type = validator.getValidatedNodeType(parsed);
        // Fully-qualified-name of each field of query
        List<List<String>> fieldOrigins = validator.getFieldOrigins(parsed);

        // Convert list of RelDataTypeField to list of ColumnMetaData
        List<RelDataTypeField> fieldList = type.getFieldList();
        List<ColumnMetaData> columns = new ArrayList<>();
        for (int i = 0; i < fieldList.size(); i++) {
            RelDataTypeField field = fieldList.get(i);
            ColumnMetaData metaData = metaData(
                    typeFactory(),
                    i,
                    field.getName(),
                    field.getType(),
                    null,
                    fieldOrigins.get(i)
            );

            columns.add(metaData);
        }
        return new Signature(
                columns,
                query,
                Collections.emptyList(), // Root query takes no parameters
                Collections.emptyMap(), // No internal parameters to keep track of
                CursorFactory.ARRAY,
                StatementType.SELECT // For now we only do SELECT queries
        );
    }

    private static SqlValidatorImpl validator() {
        return new SqlValidatorImpl(OPERATORS, catalogReader(), typeFactory(), SqlConformance.PRAGMATIC_2003) {
            // No overrides
        };
    }

    private static SqlOperatorTable createOperatorTable() {
        // Register custom macros
        registerMacro("echo", new TableMacro() {
            @Override
            public TranslatableTable apply(List<Object> arguments) {
                Object row = new Object() {
                    public String message = (String) arguments.get(0);
                };

                return new MockTable(row.getClass(), new Object[] { row });
            }

            @Override
            public List<FunctionParameter> getParameters() {
                return Collections.singletonList(
                        simpleMacroParameter("message", String.class)
                );
            }
        });

        // .instance() initializes SqlStdOperatorTable by scanning its own public static fields
        SqlStdOperatorTable standard = SqlStdOperatorTable.instance();

        // Combine custom and standard operators, with custom operators shadowing standard operators
        return new ShadowOperatorTable(CUSTOM_OPERATORS, standard);
    }

    /**
     * Register a table macro as a custom operator, so it can be used in queries like SELECT * FROM TABLE(macro(...))
     */
    public static void registerMacro(String name, TableMacro macro) {
        CUSTOM_OPERATORS.register(tableMacro(name, macro));
    }

    private static FunctionParameter simpleMacroParameter(final String name, Class<?> type) {
        RelDataType relType = typeFactory().createJavaType(type);

        return new FunctionParameter() {
            @Override
            public int getOrdinal() {
                return 0;
            }

            @Override
            public String getName() {
                return name;
            }

            @Override
            public RelDataType getType(RelDataTypeFactory typeFactory) {
                return relType;
            }

            @Override
            public boolean isOptional() {
                return false;
            }
        };
    }

    /**
     * Based on {@link CalciteCatalogReader#toOp(SqlIdentifier, org.apache.calcite.schema.Function)}
     */
    private static SqlUserDefinedTableMacro tableMacro(String name, TableMacro function) {
        JavaTypeFactory typeFactory = typeFactory();
        List<RelDataType> argTypes = new ArrayList<>();
        List<SqlTypeFamily> typeFamilies = new ArrayList<>();

        for (FunctionParameter o : function.getParameters()) {
            RelDataType type = o.getType(typeFactory);

            argTypes.add(type);
            typeFamilies.add(Util.first(type.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
        }

        List<RelDataType> paramTypes = Lists.transform(argTypes, typeFactory::toSql);
        Predicate<Integer> optional = input -> function.getParameters().get(input).isOptional();
        FamilyOperandTypeChecker typeChecker = OperandTypes.family(typeFamilies, optional);

        return new SqlUserDefinedTableMacro(
                new SqlIdentifier(name, SqlParserPos.ZERO),
                ReturnTypes.CURSOR,
                InferTypes.explicit(argTypes),
                typeChecker,
                paramTypes,
                function
        );
    }

    private static Prepare.CatalogReader catalogReader() {
        JavaTypeFactory types = typeFactory();
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);

        return new CalciteCatalogReader(rootSchema, true, Collections.emptyList(), types);
    }

    public static TruffleTypeFactory typeFactory() {
        return new TruffleTypeFactory();
    }

    private static RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        throw new UnsupportedOperationException();
    }

    public static RelRoot plan(SqlNode parsed) {
        VolcanoPlanner planner = new VolcanoPlanner(null, new TrufflePlannerContext());

        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        planner.addRule(RuleConvertProject.INSTANCE);
        planner.addRule(RuleConvertUnion.INSTANCE);
        planner.addRule(RuleConvertValues.INSTANCE);

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory()));
        SqlToRelConverter.Config config = SqlToRelConverter.configBuilder().withTrimUnusedFields(true).build();
        SqlToRelConverter converter = new SqlToRelConverter(
                TruffleMeta::expandView,
                validator(),
                catalogReader(),
                cluster,
                StandardConvertletTable.INSTANCE,
                config
        );

        RelRoot root = converter.convertQuery(parsed, true, true);
        Program program = Programs.standard();

        // program.setExecutor(?) ??

        RelTraitSet traits = root.rel.getTraitSet()
                .replace(PhysicalRel.CONVENTION)
                .replace(root.collation)
                .simplify();

        RelNode optimized = program.run(planner, root.rel, traits);

        return root.withRel(optimized);
    }

    private final Map<StatementHandle, Running> runningQueries = new ConcurrentHashMap<>();

    private static class Running {
        public final List<Object[]> rows;
        public final RelDataType type;

        private Running(List<Object[]> rows, RelDataType type) {
            this.rows = rows;
            this.type = type;
        }
    }

    /**
     * Start running a query
     */
    private void start(StatementHandle handle, RelRoot plan) {
        // Create a program that sticks query results in a list
        List<Object[]> results = new ArrayList<>();

        // Compile the query plan into a Truffle program
        CallTarget program = TruffleSqlLanguage.INSTANCE.compileInteractiveQuery(plan, results::add);

        TruffleSqlLanguage.callWithRootContext(program);

        // Stash the list so fetch(StatementHandle) can get it
        runningQueries.put(handle, new Running(results, plan.validatedRowType));
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle h,
                                           String sql,
                                           long maxRowCount,
                                           PrepareCallback callback) throws NoSuchStatementException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle h,
                                           String sql,
                                           long maxRowCount,
                                           int maxRowsInFirstFrame,
                                           PrepareCallback callback) throws NoSuchStatementException {
        SqlNode parsed = parse(sql);
        Signature signature = validate(sql, parsed);
        RelRoot plan = plan(parsed);

        start(h, plan);

        try {
            synchronized (callback.getMonitor()) {
                callback.clear();
                callback.assign(signature, null, -1);
            }

            callback.execute();

            MetaResultSet metaResultSet = MetaResultSet.create(h.connectionId, h.id, false, signature, null);

            return new ExecuteResult(Collections.singletonList(metaResultSet));
        } catch(SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle h,
                                                     List<String> sqlCommands) throws NoSuchStatementException {
        return null;
    }

    @Override
    public ExecuteBatchResult executeBatch(StatementHandle h,
                                           List<List<TypedValue>> parameterValues) throws NoSuchStatementException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Frame fetch(StatementHandle h,
                       long offset,
                       int fetchMaxRowCount) throws NoSuchStatementException, MissingResultsException {
        Running running = runningQueries.get(h);
        List<Object> slice = running.rows
                .stream()
                .skip(offset)
                .limit(fetchMaxRowCount)
                .collect(Collectors.toList());

        return new Frame(offset, slice.isEmpty(), slice);
    }

    @Override
    public ExecuteResult execute(StatementHandle h,
                                 List<TypedValue> parameterValues,
                                 long maxRowCount) throws NoSuchStatementException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExecuteResult execute(StatementHandle h,
                                 List<TypedValue> parameterValues,
                                 int maxRowsInFirstFrame) throws NoSuchStatementException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeStatement(StatementHandle h) {
        // Nothing to do
    }

    @Override
    public boolean syncResults(StatementHandle sh,
                               QueryState state,
                               long offset) throws NoSuchStatementException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(ConnectionHandle ch) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback(ConnectionHandle ch) {
        throw new UnsupportedOperationException();
    }

    private static ColumnMetaData metaData(JavaTypeFactory typeFactory,
                                           int ordinal,
                                           String fieldName,
                                           RelDataType type,
                                           RelDataType fieldType,
                                           List<String> origins) {
        type = com.fivetran.truffle.Types.simplify(type);

        final ColumnMetaData.AvaticaType avaticaType =
                avaticaType(typeFactory, type, fieldType);
        return new ColumnMetaData(
                ordinal,
                false,
                true,
                false,
                false,
                type.isNullable()
                        ? DatabaseMetaData.columnNullable
                        : DatabaseMetaData.columnNoNulls,
                true,
                type.getPrecision(),
                fieldName,
                origin(origins, 0),
                origin(origins, 2),
                getPrecision(type),
                getScale(type),
                origin(origins, 1),
                null,
                avaticaType,
                true,
                false,
                false,
                avaticaType.columnClassName());
    }

    private static ColumnMetaData.AvaticaType avaticaType(JavaTypeFactory typeFactory,
                                                          RelDataType type,
                                                          RelDataType fieldType) {
        final String typeName = getTypeName(type);
        if (type.getComponentType() != null) {
            final ColumnMetaData.AvaticaType componentType =
                    avaticaType(typeFactory, type.getComponentType(), null);
            final Type clazz = typeFactory.getJavaClass(type.getComponentType());
            final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(clazz);
            assert rep != null;
            return ColumnMetaData.array(componentType, typeName, rep);
        } else {
            final int typeOrdinal = getTypeOrdinal(type);
            switch (typeOrdinal) {
                case Types.STRUCT:
                    final List<ColumnMetaData> columns = new ArrayList<>();
                    for (RelDataTypeField field : type.getFieldList()) {
                        columns.add(metaData(typeFactory, field.getIndex(), field.getName(), field.getType(), null, null));
                    }
                    return ColumnMetaData.struct(columns);
                default:
                    final Type clazz =
                            typeFactory.getJavaClass(Util.first(fieldType, type));
                    final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of(clazz);
                    assert rep != null;
                    return ColumnMetaData.scalar(typeOrdinal, typeName, rep);
            }
        }
    }

    private static String origin(List<String> origins, int offsetFromEnd) {
        return origins == null || offsetFromEnd >= origins.size()
                ? null
                : origins.get(origins.size() - 1 - offsetFromEnd);
    }

    private static int getTypeOrdinal(RelDataType type) {
        return type.getSqlTypeName().getJdbcOrdinal();
    }

    /** Returns the type name in string form. Does not include precision, scale
     * or whether nulls are allowed. Example: "DECIMAL" not "DECIMAL(7, 2)";
     * "INTEGER" not "JavaType(int)". */
    private static String getTypeName(RelDataType type) {
        final SqlTypeName sqlTypeName = type.getSqlTypeName();
        switch (sqlTypeName) {
            case ARRAY:
            case MULTISET:
            case MAP:
            case ROW:
                return type.toString(); // e.g. "INTEGER ARRAY"
            case INTERVAL_YEAR_MONTH:
                return "INTERVAL_YEAR_TO_MONTH";
            case INTERVAL_DAY_HOUR:
                return "INTERVAL_DAY_TO_HOUR";
            case INTERVAL_DAY_MINUTE:
                return "INTERVAL_DAY_TO_MINUTE";
            case INTERVAL_DAY_SECOND:
                return "INTERVAL_DAY_TO_SECOND";
            case INTERVAL_HOUR_MINUTE:
                return "INTERVAL_HOUR_TO_MINUTE";
            case INTERVAL_HOUR_SECOND:
                return "INTERVAL_HOUR_TO_SECOND";
            case INTERVAL_MINUTE_SECOND:
                return "INTERVAL_MINUTE_TO_SECOND";
            default:
                return sqlTypeName.getName(); // e.g. "DECIMAL", "INTERVAL_YEAR_MONTH"
        }
    }

    private static int getScale(RelDataType type) {
        return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED
                ? 0
                : type.getScale();
    }

    private static int getPrecision(RelDataType type) {
        return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
                ? 0
                : type.getPrecision();
    }
}
