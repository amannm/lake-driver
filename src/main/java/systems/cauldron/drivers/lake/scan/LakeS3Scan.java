package systems.cauldron.drivers.lake.scan;

import software.amazon.awssdk.services.s3.S3Client;
import systems.cauldron.drivers.lake.config.FormatSpec;
import systems.cauldron.drivers.lake.config.TypeSpec;

import java.net.URI;

abstract class LakeS3Scan extends LakeScan {

    final S3Client s3 = S3Client.builder().build();

    LakeS3Scan(URI source, FormatSpec format, TypeSpec[] types, int[] projects) {
        super(source, format, types, projects);
    }
}
