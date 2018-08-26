package systems.cauldron.drivers.provider;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.sql.SqlKind.INPUT_REF;
import static org.apache.calcite.sql.SqlKind.LITERAL;

public class LakeS3SelectFilterTranslator implements LakeFilterTranslator {

    //TODO: cover full subset of SQL that Amazon S3 Select currently supports
    //TODO: this part needs a lot of thought and development to avoid bugs and edge case
    //TODO: don't be too aggressive with pushing down filters when you're unsure of its ability to handle weird SQL
    @Override
    public String compileQuery(List<RexNode> filters, int[] projects) {
        String where = compileNativeFilterString(filters);
        String selectList = IntStream.of(projects).boxed()
                .map(i -> i + 1).map(i -> "_" + i)
                .collect(Collectors.joining(", "));
        return String.format("SELECT %s FROM S3Object", selectList) + where;
    }

    private static String compileNativeFilterString(List<RexNode> filters) {

        List<String> handledFilters = new ArrayList<>();
        List<RexNode> unhandledFilters = new ArrayList<>();

        for (Iterator<RexNode> filterIterator = filters.iterator(); filterIterator.hasNext(); ) {
            RexNode filter = filterIterator.next();
            if (RelOptUtil.disjunctions(filter).size() == 1) {
                List<RexNode> conjunctions = RelOptUtil.conjunctions(filter);
                for (RexNode conjunction : conjunctions) {
                    Optional<String> handledFilterString = tryFilterConversion(conjunction);
                    if (handledFilterString.isPresent()) {
                        handledFilters.add(handledFilterString.get());
                    } else {
                        unhandledFilters.add(conjunction);
                    }
                }
                filterIterator.remove();
            } else {
                //TODO: push down ORs as well
            }
        }
        filters.addAll(unhandledFilters);

        if (handledFilters.isEmpty()) {
            return "";
        }

        return " WHERE " + String.join(" AND ", handledFilters);
    }

    private static Optional<String> tryFilterConversion(RexNode node) {
        RexCall call = (RexCall) node;
        switch (call.getKind()) {
            case NOT_EQUALS:
                return tryOperatorFilterConversion("<>", call);
            case EQUALS:
                return tryOperatorFilterConversion("=", call);
            case LESS_THAN:
                return tryOperatorFilterConversion("<", call);
            case LESS_THAN_OR_EQUAL:
                return tryOperatorFilterConversion("<=", call);
            case GREATER_THAN:
                return tryOperatorFilterConversion(">", call);
            case GREATER_THAN_OR_EQUAL:
                return tryOperatorFilterConversion(">=", call);
            default:
                return Optional.empty();
        }
    }

    private static Optional<String> tryOperatorFilterConversion(String op, RexCall call) {
        RexNode originalLeft = call.operands.get(0);
        RexNode originalRight = call.operands.get(1);

        RexNode left = unwrapCasts(originalLeft);
        RexNode right = unwrapCasts(originalRight);

        final String fieldName;
        final String literal;
        if (isSimpleLiteralColumnValueFilter(left, right)) {
            fieldName = String.format(generateCastTemplate(originalLeft), compileFieldName(left));
            literal = String.format(generateCastTemplate(originalRight), compileLiteral(right));
        } else {
            if (isSimpleLiteralColumnValueFilter(right, left)) {
                fieldName = String.format(generateCastTemplate(originalRight), compileFieldName(right));
                literal = String.format(generateCastTemplate(originalLeft), compileLiteral(left));
            } else {
                return Optional.empty();
            }
        }
        return Optional.of(String.format("%s %s %s", fieldName, op, literal));

    }

    private static boolean isSimpleLiteralColumnValueFilter(RexNode left, RexNode right) {
        return left.getKind() == INPUT_REF && right.getKind() == LITERAL;
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
