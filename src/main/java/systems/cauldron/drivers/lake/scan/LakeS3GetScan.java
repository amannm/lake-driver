package systems.cauldron.drivers.lake.scan;

import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import systems.cauldron.drivers.lake.config.FormatSpec;
import systems.cauldron.drivers.lake.config.TypeSpec;
import systems.cauldron.drivers.lake.converter.NonProjectedStringRowConverter;
import systems.cauldron.drivers.lake.converter.StringRowConverter;

import java.io.InputStream;
import java.net.URI;

public class LakeS3GetScan extends LakeS3Scan {

    LakeS3GetScan(URI source, FormatSpec format, TypeSpec[] types, int[] projects) {
        super(types, projects, source, format);
    }

    @Override
    public StringRowConverter getStringRowConverter() {
        return new NonProjectedStringRowConverter(types, projects);
    }

    @Override
    public InputStream getSource() {
        AmazonS3URI amazonS3URI = new AmazonS3URI(source);
        return s3.getObject(GetObjectRequest.builder()
                        .bucket(amazonS3URI.getBucket())
                        .key(amazonS3URI.getKey())
                        .build(),
                ResponseTransformer.toInputStream());
    }

}
