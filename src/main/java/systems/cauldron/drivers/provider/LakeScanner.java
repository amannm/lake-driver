package systems.cauldron.drivers.provider;

import org.apache.calcite.rex.RexNode;
import systems.cauldron.drivers.config.TableSpec;
import systems.cauldron.drivers.config.TypeSpec;

import java.util.List;

public interface LakeScanner {

    LakeScan scan(int[] projects, List<RexNode> filters);

    static LakeScanner create(Class<?> providerClass, TableSpec spec) {
        TypeSpec[] fields = spec.columns.stream().map(c -> c.datatype).toArray(TypeSpec[]::new);
        if (LakeS3SelectScan.class.equals(providerClass)) {
            return (projects, filters) -> new LakeS3SelectScan(
                    fields,
                    projects,
                    filters,
                    spec.location,
                    spec.format
            );
        }
        if (LakeS3GetScan.class.equals(providerClass)) {
            return (projects, filters) -> new LakeS3GetScan(
                    fields,
                    projects,
                    spec.location
            );
        }
        throw new IllegalArgumentException("encountered unknown provider class: " + providerClass.getName());
    }

}
