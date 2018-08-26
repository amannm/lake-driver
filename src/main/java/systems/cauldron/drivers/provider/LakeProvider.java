package systems.cauldron.drivers.provider;

import systems.cauldron.drivers.adapter.LakeFieldType;
import systems.cauldron.drivers.config.TableSpecification;

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

    public static LakeProviderFactory getFactory(TableSpecification specification, Class<?> providerClass) {
        if (LakeS3SelectProvider.class.equals(providerClass)) {
            return (filters, projects, fieldTypes) -> new LakeS3SelectProvider(specification.location, specification.format, filters, projects, fieldTypes);
        }
        if (LakeS3GetProvider.class.equals(providerClass)) {
            return (filters, projects, fieldTypes) -> new LakeS3GetProvider(specification.location, projects, fieldTypes);
        }
        throw new IllegalArgumentException("encountered unknown provider class: " + providerClass.getName());
    }

}
