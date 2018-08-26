package systems.cauldron.drivers.provider;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import systems.cauldron.drivers.config.FormatSpecification;

import java.io.InputStream;
import java.net.URI;

public class LakeS3SelectGateway extends LakeGateway {

    public LakeS3SelectGateway(URI source, FormatSpecification format, String query) {
        super(source, format, query);
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

    private InputSerialization getInputSerialization(FormatSpecification spec) {

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

    private OutputSerialization getOutputSerialization(FormatSpecification spec) {

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
