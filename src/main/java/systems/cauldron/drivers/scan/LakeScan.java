package systems.cauldron.drivers.scan;

import systems.cauldron.drivers.config.FormatSpec;
import systems.cauldron.drivers.config.TypeSpec;
import systems.cauldron.drivers.converter.RowConverter;

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
