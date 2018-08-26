package systems.cauldron.drivers.provider;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.S3Object;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import systems.cauldron.drivers.adapter.LakeFieldType;

import java.io.InputStream;
import java.net.URI;

public class LakeS3GetProvider extends LakeProvider {

    private static final Logger LOG = LoggerFactory.getLogger(LakeS3GetProvider.class);

    public LakeS3GetProvider(URI source, int[] projects, LakeFieldType[] fieldTypes) {
        super(source, projects, fieldTypes);
    }

    @Override
    public InputStream fetchSource() {
        final AmazonS3URI sourceUri = new AmazonS3URI(source);
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        S3Object o = s3.getObject(sourceUri.getBucket(), sourceUri.getKey());
        return o.getObjectContent();
    }

    @Override
    public int[] getProjects() {
        return projects;
    }

    @Override
    public LakeFieldType[] getFieldTypes() {
        return fieldTypes;
    }


}
