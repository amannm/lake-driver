package systems.cauldron.drivers.scan;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import systems.cauldron.drivers.config.FormatSpec;
import systems.cauldron.drivers.config.TypeSpec;

import java.net.URI;

public abstract class LakeS3Scan extends LakeScan {

    final AmazonS3URI s3Source = new AmazonS3URI(source);
    final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

    LakeS3Scan(TypeSpec[] types, int[] projects, URI source, FormatSpec format) {
        super(types, projects, source, format);
    }
}
