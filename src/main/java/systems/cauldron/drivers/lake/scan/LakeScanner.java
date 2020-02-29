package systems.cauldron.drivers.lake.scan;

import org.apache.calcite.rex.RexNode;
import systems.cauldron.drivers.lake.config.TableSpec;
import systems.cauldron.drivers.lake.config.TypeSpec;

import java.util.List;
import java.util.function.BiFunction;

public interface LakeScanner extends BiFunction<int[], List<RexNode>, LakeScan> {

    static LakeScanner create(Class<?> providerClass, TableSpec spec) {
        TypeSpec[] fields = spec.columns.stream().map(c -> c.datatype).toArray(TypeSpec[]::new);
        if (LakeS3SelectWhereScan.class.equals(providerClass)) {
            return (projects, filters) -> new LakeS3SelectWhereScan(
                    spec.location, spec.format, fields,
                    projects,
                    filters
            );
        }
        if (LakeS3SelectScan.class.equals(providerClass)) {
            return (projects, filters) -> new LakeS3SelectScan(
                    spec.location, spec.format, fields,
                    projects
            );
        }
        if (LakeS3GetScan.class.equals(providerClass)) {
            return (projects, filters) -> new LakeS3GetScan(
                    spec.location, spec.format, fields,
                    projects
            );
        }
        throw new IllegalArgumentException("encountered unknown provider class: " + providerClass.getName());
    }

}
