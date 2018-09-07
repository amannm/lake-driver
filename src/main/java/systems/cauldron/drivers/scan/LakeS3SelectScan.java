package systems.cauldron.drivers.scan;

import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import systems.cauldron.drivers.config.FormatSpec;
import systems.cauldron.drivers.config.TypeSpec;
import systems.cauldron.drivers.converter.ProjectedRowConverter;
import systems.cauldron.drivers.converter.RowConverter;

import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LakeS3SelectScan extends LakeS3Scan {

    private static final Logger LOG = LoggerFactory.getLogger(LakeS3SelectScan.class);

    private final String query;

    LakeS3SelectScan(URI source, FormatSpec format, TypeSpec[] fieldTypes, int[] projects) {
        super(source, format, fieldTypes, projects);
        this.query = compileSelectFromClause(projects);
        LOG.debug("{}", query);
    }

    LakeS3SelectScan(URI source, FormatSpec format, TypeSpec[] fieldTypes, int[] projects, List<RexNode> filters) {
        super(source, format, fieldTypes, projects);
        this.query = compileQuery(projects, filters);
        LOG.debug("{}", query);
    }

    @Override
    public RowConverter getRowConverter() {

        TypeSpec[] projectedTypes = IntStream.of(projects).boxed()
                .map(i -> types[i])
                .toArray(TypeSpec[]::new);

        return new ProjectedRowConverter(projectedTypes);

    }

    @Override
    public InputStream getSource() {

        SelectObjectContentRequest request = getRequest(s3Source, query, format);
        SelectObjectContentResult result = s3Client.selectObjectContent(request);
        SelectObjectContentEventStream payload = result.getPayload();

        return payload.getRecordsInputStream();

    }

    protected String compileQuery(int[] projects, List<RexNode> filters) {
        return compileSelectFromClause(projects);
    }

    static String compileSelectFromClause(int[] projects) {

        String selectList = IntStream.of(projects).boxed()
                .map(i -> i + 1).map(i -> "_" + i)
                .collect(Collectors.joining(", "));

        return String.format("SELECT %s FROM S3Object", selectList);

    }

    private static SelectObjectContentRequest getRequest(AmazonS3URI uri, String query, FormatSpec format) {

        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(uri.getBucket());
        request.setKey(uri.getKey());
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);
        request.setInputSerialization(getInputSerialization(format));
        request.setOutputSerialization(getOutputSerialization(format));

        return request;

    }

    private static InputSerialization getInputSerialization(FormatSpec spec) {

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

    private static OutputSerialization getOutputSerialization(FormatSpec spec) {

        CSVOutput csvOutput = new CSVOutput();
        csvOutput.setFieldDelimiter(spec.delimiter);
        csvOutput.setRecordDelimiter(spec.lineSeparator);
        csvOutput.setQuoteCharacter(spec.quoteChar);
        csvOutput.setQuoteEscapeCharacter(spec.escape);

        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setCsv(csvOutput);

        return outputSerialization;

    }


}
