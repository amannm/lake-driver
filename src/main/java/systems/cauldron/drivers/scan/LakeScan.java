package systems.cauldron.drivers.scan;

import systems.cauldron.drivers.config.FormatSpec;
import systems.cauldron.drivers.config.TypeSpec;
import systems.cauldron.drivers.converter.RowConverter;

import java.io.InputStream;
import java.net.URI;

public abstract class LakeScan {

    final TypeSpec[] types;
    final int[] projects;
    final URI source;
    final FormatSpec format;

    LakeScan(TypeSpec[] types, int[] projects, URI source, FormatSpec format) {
        this.types = types;
        this.projects = projects;
        this.source = source;
        this.format = format;
    }

    public abstract RowConverter getRowConverter();

    public abstract InputStream getSource();

    public FormatSpec getFormat() {
        return format;
    }

}
