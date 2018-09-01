package systems.cauldron.drivers.provider;

import systems.cauldron.drivers.config.TypeSpecification;

import java.io.InputStream;
import java.net.URI;

public abstract class LakeProvider {

    protected final URI source;
    protected final int[] projects;
    protected final TypeSpecification[] fieldTypes;


    protected LakeProvider(URI source, int[] projects, TypeSpecification[] fieldTypes) {
        this.source = source;
        this.projects = projects;
        this.fieldTypes = fieldTypes;
    }

    public abstract InputStream fetchSource();

    public abstract int[] getProjects();

    public abstract TypeSpecification[] getFieldTypes();


}
