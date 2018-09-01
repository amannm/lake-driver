package systems.cauldron.drivers.provider;

import org.apache.calcite.rex.RexNode;
import systems.cauldron.drivers.config.TableSpec;
import systems.cauldron.drivers.config.TypeSpec;

import java.util.List;

public interface LakeScanner {

    LakeScan scan(TypeSpec[] fieldTypes, int[] projects, List<RexNode> filters);

    static LakeScanner create(Class<?> providerClass, TableSpec specification) {
        if (LakeS3SelectScan.class.equals(providerClass)) {
            return (fieldTypes, projects, filters) -> new LakeS3SelectScan(
                    fieldTypes,
                    projects,
                    filters,
                    specification.location,
                    specification.format
            );
        }
        if (LakeS3GetScan.class.equals(providerClass)) {
            return (fieldTypes, projects, filters) -> new LakeS3GetScan(
                    fieldTypes,
                    projects,
                    specification.location
            );
        }
        throw new IllegalArgumentException("encountered unknown provider class: " + providerClass.getName());
    }

}
