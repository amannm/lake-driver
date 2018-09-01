package systems.cauldron.drivers.provider;

import org.apache.calcite.rex.RexNode;
import systems.cauldron.drivers.config.TableSpecification;
import systems.cauldron.drivers.config.TypeSpecification;

import java.util.List;

public interface LakeProviderFactory {

    LakeProvider build(List<RexNode> filters, int[] projects, TypeSpecification[] fieldTypes);

    static LakeProviderFactory create(Class<?> providerClass, TableSpecification specification) {
        if (LakeS3SelectProvider.class.equals(providerClass)) {
            return (filters, projects, fieldTypes) -> new LakeS3SelectProvider(specification.location, projects, fieldTypes, specification.format, filters);
        }
        if (LakeS3GetProvider.class.equals(providerClass)) {
            return (filters, projects, fieldTypes) -> new LakeS3GetProvider(specification.location, projects, fieldTypes);
        }
        throw new IllegalArgumentException("encountered unknown provider class: " + providerClass.getName());
    }

}
