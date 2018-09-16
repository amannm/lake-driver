package systems.cauldron.drivers.lake.scan;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import systems.cauldron.drivers.lake.config.FormatSpec;
import systems.cauldron.drivers.lake.config.TypeSpec;

import java.net.URI;

abstract class LakeS3Scan extends LakeScan {

    final AmazonS3URI s3Source = new AmazonS3URI(source);
    final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

    LakeS3Scan(URI source, FormatSpec format, TypeSpec[] types, int[] projects) {
        super(source, format, types, projects);
    }
}
