package systems.cauldron.drivers.provider;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;

public class LakeS3Provider extends LakeProvider {

    private static final Logger LOG = LoggerFactory.getLogger(LakeS3Provider.class);

    public LakeS3Provider(URI source) {
        super(source);
    }

    @Override
    public InputStream fetchSource() {
        final AmazonS3URI sourceUri = new AmazonS3URI(source);
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        S3Object o = s3.getObject(sourceUri.getBucket(), sourceUri.getKey());
        return o.getObjectContent();
    }

    @Override
    public boolean hasProjectedResults() {
        return false;
    }

}
