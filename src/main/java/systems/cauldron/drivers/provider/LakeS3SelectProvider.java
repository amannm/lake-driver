package systems.cauldron.drivers.provider;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import systems.cauldron.drivers.adapter.LakeFieldType;
import systems.cauldron.drivers.config.FormatSpecification;

import java.io.InputStream;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.sql.SqlKind.INPUT_REF;
import static org.apache.calcite.sql.SqlKind.LITERAL;

public class LakeS3SelectProvider extends LakeProvider {

    private static final Logger LOG = LoggerFactory.getLogger(LakeS3SelectProvider.class);

    private final FormatSpecification format;
    private final String query;

    public LakeS3SelectProvider(URI source, int[] projects, LakeFieldType[] fieldTypes, FormatSpecification format, List<RexNode> filters) {
        super(source, projects, fieldTypes);
        this.format = format;
        this.query = compileQuery(filters, projects);
        LOG.info("{}", query);
    }

    @Override
    public InputStream fetchSource() {

        AmazonS3URI amazonS3URI = new AmazonS3URI(source);

        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(amazonS3URI.getBucket());
        request.setKey(amazonS3URI.getKey());
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);
        request.setInputSerialization(getInputSerialization(format));
        request.setOutputSerialization(getOutputSerialization(format));

        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        SelectObjectContentResult result = s3.selectObjectContent(request);
        SelectObjectContentEventStream payload = result.getPayload();
        return payload.getRecordsInputStream();
    }

    @Override
    public int[] getProjects() {
        return IntStream.range(0, projects.length).toArray();
    }

    @Override
    public LakeFieldType[] getFieldTypes() {
        return IntStream.of(projects).boxed().map(i -> fieldTypes[i]).toArray(LakeFieldType[]::new);
    }

    private static InputSerialization getInputSerialization(FormatSpecification spec) {

        CSVInput csvInput = new CSVInput();
        csvInput.setFieldDelimiter(spec.delimiter);
        csvInput.setRecordDelimiter(spec.lineSeparator);
        csvInput.setQuoteCharacter(spec.quoteChar);
        csvInput.setQuoteEscapeCharacter(spec.escape);
        csvInput.setFileHeaderInfo(spec.header ? FileHeaderInfo.USE : FileHeaderInfo.NONE);
        csvInput.setComments(spec.commentChar);

        InputSerialization inputSerialization = new InputSerialization();
        inputSerialization.setCsv(csvInput);
        switch (spec.compression) {
            case GZIP:
                inputSerialization.setCompressionType(CompressionType.GZIP);
                break;
            case NONE:
            default:
                inputSerialization.setCompressionType(CompressionType.NONE);
        }
        return inputSerialization;

    }

    private static OutputSerialization getOutputSerialization(FormatSpecification spec) {

        CSVOutput csvOutput = new CSVOutput();
        csvOutput.setFieldDelimiter(spec.delimiter);
        csvOutput.setRecordDelimiter(spec.lineSeparator);
        csvOutput.setQuoteCharacter(spec.quoteChar);
        csvOutput.setQuoteEscapeCharacter(spec.escape);

        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setCsv(csvOutput);
        return outputSerialization;

    }

    private static String compileQuery(List<RexNode> filters, int[] projects) {
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
