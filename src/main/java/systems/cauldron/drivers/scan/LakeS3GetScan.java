package systems.cauldron.drivers.scan;

import com.amazonaws.services.s3.model.S3Object;
import systems.cauldron.drivers.config.FormatSpec;
import systems.cauldron.drivers.config.TypeSpec;
import systems.cauldron.drivers.converter.NonProjectedRowConverter;
import systems.cauldron.drivers.converter.RowConverter;

import java.io.InputStream;
import java.net.URI;

public class LakeS3GetScan extends LakeS3Scan {

    LakeS3GetScan(URI source, FormatSpec format, TypeSpec[] types, int[] projects) {
        super(source, format, types, projects);
    }

    @Override
    public RowConverter getRowConverter() {
        return new NonProjectedRowConverter(types, projects);
    }

    @Override
    public InputStream getSource() {
        S3Object result = s3Client.getObject(s3Source.getBucket(), s3Source.getKey());
        return result.getObjectContent();
    }

}
