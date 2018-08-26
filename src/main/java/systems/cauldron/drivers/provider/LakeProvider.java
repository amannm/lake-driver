package systems.cauldron.drivers.provider;

import systems.cauldron.drivers.adapter.LakeFieldType;

import java.io.InputStream;
import java.net.URI;

public abstract class LakeProvider {

    protected final URI source;
    protected final int[] projects;
    protected final LakeFieldType[] fieldTypes;


    protected LakeProvider(URI source, int[] projects, LakeFieldType[] fieldTypes) {
        this.source = source;
        this.projects = projects;
        this.fieldTypes = fieldTypes;
    }

    public abstract InputStream fetchSource();

    public abstract int[] getProjects();

    public abstract LakeFieldType[] getFieldTypes();
}
