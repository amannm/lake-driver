package systems.cauldron.drivers.scan;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import systems.cauldron.drivers.config.FormatSpec;
import systems.cauldron.drivers.config.TypeSpec;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Stack;

import static org.apache.calcite.sql.SqlKind.INPUT_REF;
import static org.apache.calcite.sql.SqlKind.LITERAL;

public class LakeS3SelectWhereScan extends LakeS3SelectScan {

    LakeS3SelectWhereScan(URI source, FormatSpec format, TypeSpec[] fieldTypes, int[] projects, List<RexNode> filters) {
        super(source, format, fieldTypes, projects, filters);
    }

    @Override
    public String compileQuery(int[] projects, List<RexNode> filters) {
        return compileQuery(projects) + compileWhereClause(filters);
    }

    private static String compileWhereClause(List<RexNode> filters) {

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
