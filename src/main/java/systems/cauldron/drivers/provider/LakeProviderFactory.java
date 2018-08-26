package systems.cauldron.drivers.provider;

import org.apache.calcite.rex.RexNode;
import systems.cauldron.drivers.adapter.LakeFieldType;
import systems.cauldron.drivers.config.TableSpecification;

import java.util.List;

public interface LakeProviderFactory {

    LakeProvider build(List<RexNode> filters, int[] projects, LakeFieldType[] fieldTypes);

    static LakeProviderFactory create(TableSpecification specification, Class<?> providerClass) {
        if (LakeS3SelectProvider.class.equals(providerClass)) {
            return (filters, projects, fieldTypes) -> new LakeS3SelectProvider(specification.location, specification.format, filters, projects, fieldTypes);
        }
        if (LakeS3GetProvider.class.equals(providerClass)) {
            return (filters, projects, fieldTypes) -> new LakeS3GetProvider(specification.location, projects, fieldTypes);
        }
        throw new IllegalArgumentException("encountered unknown provider class: " + providerClass.getName());
    }

}
