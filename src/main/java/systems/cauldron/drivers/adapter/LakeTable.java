package systems.cauldron.drivers.adapter;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import systems.cauldron.drivers.config.ColumnSpecification;
import systems.cauldron.drivers.config.FormatSpecification;
import systems.cauldron.drivers.config.TableSpecification;

import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.sql.SqlKind.INPUT_REF;
import static org.apache.calcite.sql.SqlKind.LITERAL;


public class LakeTable extends AbstractTable implements ProjectableFilterableTable {

    private static final Logger LOG = LoggerFactory.getLogger(LakeTable.class);

    private final String label;
    private final URI source;
    private final List<ColumnSpecification> columns;
    private final FormatSpecification format;

    public LakeTable(TableSpecification specification) {
        this.label = specification.getLabel().toUpperCase();
        this.source = specification.getLocation();
        this.columns = specification.getColumns();
        this.format = specification.getFormat();
    }

    public String getLabel() {
        return label;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (ColumnSpecification c : columns) {
            RelDataType relDataType = LakeFieldType.of(c.getDatatype()).toType(typeFactory);
            builder.add(c.getLabel().toUpperCase(), relDataType);
            builder.nullable(c.isNullable());
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        projects = projects == null ? IntStream.range(0, columns.size()).toArray() : projects;
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        final String s3SelectQuery = compileS3SelectQuery(filters, projects);
        final LakeFieldType[] fieldTypes = generateFieldList(projects);

        LOG.info("{}", s3SelectQuery);

        return new AbstractEnumerable<>() {
            public Enumerator<Object[]> enumerator() {
                return new LakeEnumerator(source, format, s3SelectQuery, fieldTypes, cancelFlag);
            }
        };
    }

    private LakeFieldType[] generateFieldList(int[] fields) {
        return IntStream.of(fields).boxed()
                .map(columns::get)
                .map(ColumnSpecification::getDatatype)
                .map(LakeFieldType::of)
                .toArray(LakeFieldType[]::new);
    }

    //TODO: cover full subset of SQL that Amazon S3 Select currently supports
    //TODO: this part needs a lot of thought and development to avoid bugs and edge case
    //TODO: don't be too aggressive with pushing down filters when you're unsure of its ability to handle weird SQL
    private static String compileS3SelectQuery(List<RexNode> filters, int[] projects) {
        String where = compileNativeFilter(filters);
        String selectList = IntStream.of(projects).boxed()
                .map(i -> i + 1).map(i -> "_" + i)
                .collect(Collectors.joining(", "));
        return String.format("SELECT %s FROM S3Object", selectList) + where;
    }

    private static String compileNativeFilter(List<RexNode> filters) {

        List<String> handledFilters = new ArrayList<>();
        List<RexNode> unhandledFilters = new ArrayList<>();

        for (Iterator<RexNode> filterIterator = filters.iterator(); filterIterator.hasNext(); ) {
            RexNode filter = filterIterator.next();
            if (RelOptUtil.disjunctions(filter).size() == 1) {
                List<RexNode> conjunctions = RelOptUtil.conjunctions(filter);
                for (RexNode conjunction : conjunctions) {
                    Optional<String> handledFilterString = addFilter(conjunction);
                    if (handledFilterString.isPresent()) {
                        handledFilters.add(handledFilterString.get());
                    } else {
                        unhandledFilters.add(conjunction);
                    }
                }
                filterIterator.remove();
            }
        }
        filters.addAll(unhandledFilters);

        if (handledFilters.isEmpty()) {
            return "";
        }

        return " WHERE " + String.join(" AND ", handledFilters);
    }

    private static Optional<String> addFilter(RexNode node) {
        RexCall call = (RexCall) node;
        switch (call.getKind()) {
            case NOT_EQUALS:
                return compileNativeFilter("<>", call);
            case EQUALS:
                return compileNativeFilter("=", call);
            case LESS_THAN:
                return compileNativeFilter("<", call);
            case LESS_THAN_OR_EQUAL:
                return compileNativeFilter("<=", call);
            case GREATER_THAN:
                return compileNativeFilter(">", call);
            case GREATER_THAN_OR_EQUAL:
                return compileNativeFilter(">=", call);
            default:
                return Optional.empty();
        }
    }

    private static Optional<String> compileNativeFilter(String op, RexCall call) {
        RexNode originalLeft = call.operands.get(0);
        RexNode originalRight = call.operands.get(1);

        RexNode left = unwrapCasts(originalLeft);
        RexNode right = unwrapCasts(originalRight);

        final String fieldName;
        final String literal;

        if (left.getKind() == LITERAL) {
            if (right.getKind() != INPUT_REF) {
                return Optional.empty();
            } else {
                fieldName = String.format(generateCastTemplate(originalRight), compileLiteral(right));
                literal = String.format(generateCastTemplate(originalLeft), compileFieldName(left));
            }
        } else if (right.getKind() == LITERAL) {
            if (left.getKind() != INPUT_REF) {
                return Optional.empty();
            } else {
                fieldName = String.format(generateCastTemplate(originalLeft), compileFieldName(left));
                literal = String.format(generateCastTemplate(originalRight), compileLiteral(right));
            }
        } else {
            return Optional.empty();
        }

        return Optional.of(String.format("%s %s %s", fieldName, op, literal));

    }

    private static RexNode unwrapCasts(RexNode node) {
        while (node.isA(SqlKind.CAST)) {
            node = ((RexCall) node).operands.get(0);
        }
        return node;
    }

    private static String generateCastTemplate(RexNode node) {
        Stack<String> stack = new Stack<>();
        while (node.isA(SqlKind.CAST)) {
            String datatype = node.getType().getSqlTypeName().getName();
            stack.push("CAST(%s AS " + datatype + ")");
            node = ((RexCall) node).operands.get(0);
        }
        String result = "%s";
        while (!stack.isEmpty()) {
            String pop = stack.pop();
            result = String.format(pop, result);
        }
        return result;
    }

    private static String compileFieldName(RexNode node) {
        int index = ((RexInputRef) node).getIndex() + 1;
        return "_" + index;
    }

    private static String compileLiteral(RexNode node) {
        RexLiteral literal = (RexLiteral) node;
        if (SqlTypeName.STRING_TYPES.contains(literal.getTypeName())) {
            return '\'' + literal.getValue2().toString() + '\'';
        }
        return literal.getValue2().toString();
    }

}