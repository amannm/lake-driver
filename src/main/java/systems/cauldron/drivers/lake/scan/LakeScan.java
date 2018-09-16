package systems.cauldron.drivers.lake.scan;

import systems.cauldron.drivers.lake.config.FormatSpec;
import systems.cauldron.drivers.lake.config.TypeSpec;
import systems.cauldron.drivers.lake.converter.RowConverter;

import java.io.InputStream;
import java.net.URI;

public abstract class LakeScan {

    final URI source;
    final FormatSpec format;
    final TypeSpec[] types;
    final int[] projects;

    LakeScan(URI source, FormatSpec format, TypeSpec[] types, int[] projects) {
        this.source = source;
        this.format = format;
        this.types = types;
        this.projects = projects;
    }

    public abstract RowConverter getRowConverter();

    public abstract InputStream getSource();

    public FormatSpec getFormat() {
        return format;
    }

}
