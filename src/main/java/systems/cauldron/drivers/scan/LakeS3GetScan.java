package systems.cauldron.drivers.scan;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import systems.cauldron.drivers.config.FormatSpec;
import systems.cauldron.drivers.config.TypeSpec;

import java.io.InputStream;
import java.net.URI;

public class LakeS3GetScan extends LakeScan {

    LakeS3GetScan(TypeSpec[] fieldTypes, int[] projects, URI source, FormatSpec format) {
        super(fieldTypes, projects, source, format);
    }

    @Override
    public InputStream getSource() {
        final AmazonS3URI sourceUri = new AmazonS3URI(source);
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        S3Object o = s3.getObject(sourceUri.getBucket(), sourceUri.getKey());
        return o.getObjectContent();
    }

}
