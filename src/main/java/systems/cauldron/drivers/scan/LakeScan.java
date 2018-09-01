package systems.cauldron.drivers.scan;

import systems.cauldron.drivers.config.FormatSpec;
import systems.cauldron.drivers.config.TypeSpec;

import java.io.InputStream;
import java.net.URI;

public abstract class LakeScan {

    final TypeSpec[] types;
    final int[] projects;
    final URI source;
    final FormatSpec format;

    public LakeScan(TypeSpec[] types, int[] projects, URI source, FormatSpec format) {
        this.types = types;
        this.projects = projects;
        this.source = source;
        this.format = format;
    }

    public TypeSpec[] getTypes() {
        return types;
    }

    public int[] getProjects() {
        return projects;
    }

    public abstract InputStream getSource();

    public FormatSpec getFormat() {
        return format;
    }

}
