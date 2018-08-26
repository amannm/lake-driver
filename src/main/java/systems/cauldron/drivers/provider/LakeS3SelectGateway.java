package systems.cauldron.drivers.provider;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import systems.cauldron.drivers.config.FormatSpecification;

import java.io.InputStream;
import java.net.URI;

public class LakeS3SelectGateway {

    public static InputStream fetchSource(URI location, FormatSpecification inputSpecification, String query) {

        //TODO: make async buffered loading (do something. unga bunga. be smurt...)

        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

        AmazonS3URI amazonS3URI = new AmazonS3URI(location);

        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(amazonS3URI.getBucket());
        request.setKey(amazonS3URI.getKey());
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);

        CSVInput csvInput = new CSVInput();
        csvInput.setFieldDelimiter(inputSpecification.delimiter);
        csvInput.setRecordDelimiter(inputSpecification.lineSeparator);
        csvInput.setQuoteCharacter(inputSpecification.quoteChar);
        csvInput.setQuoteEscapeCharacter(inputSpecification.escape);
        csvInput.setFileHeaderInfo(inputSpecification.header ? FileHeaderInfo.USE : FileHeaderInfo.NONE);
        csvInput.setComments(inputSpecification.commentChar);

        InputSerialization inputSerialization = new InputSerialization();
        inputSerialization.setCsv(csvInput);
        switch (inputSpecification.compression) {
            case GZIP:
                inputSerialization.setCompressionType(CompressionType.GZIP);
                break;
            case NONE:
            default:
                inputSerialization.setCompressionType(CompressionType.NONE);
        }

        request.setInputSerialization(inputSerialization);

        CSVOutput csvOutput = new CSVOutput();
        csvOutput.setFieldDelimiter(inputSpecification.delimiter);
        csvOutput.setRecordDelimiter(inputSpecification.lineSeparator);
        csvOutput.setQuoteCharacter(inputSpecification.quoteChar);
        csvOutput.setQuoteEscapeCharacter(inputSpecification.escape);

        OutputSerialization outputSerialization = new OutputSerialization();
        outputSerialization.setCsv(csvOutput);

        request.setOutputSerialization(outputSerialization);

        SelectObjectContentResult result = s3.selectObjectContent(request);
        return result.getPayload().getRecordsInputStream();
    }
}
