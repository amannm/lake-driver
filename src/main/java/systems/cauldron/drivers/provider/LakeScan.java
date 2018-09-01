package systems.cauldron.drivers.provider;

import systems.cauldron.drivers.config.TypeSpec;

import java.io.InputStream;
import java.net.URI;

public abstract class LakeScan {

    protected final URI source;
    protected final int[] projects;
    protected final TypeSpec[] fieldTypes;


    protected LakeScan(TypeSpec[] fieldTypes, int[] projects, URI source) {
        this.source = source;
        this.projects = projects;
        this.fieldTypes = fieldTypes;
    }

    public abstract InputStream fetchSource();

    public abstract int[] getProjects();

    public abstract TypeSpec[] getFieldTypes();


}
