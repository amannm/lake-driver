package systems.cauldron.drivers.adapter;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import systems.cauldron.drivers.config.TableSpecification;
import systems.cauldron.drivers.provider.LakeS3GetProvider;
import systems.cauldron.drivers.provider.LakeS3SelectProvider;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LakeSchemaFactory implements SchemaFactory {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSchemaFactory.class);

    public static final LakeSchemaFactory INSTANCE = new LakeSchemaFactory();

    public LakeSchemaFactory() {
    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        String inputTablesJsonArrayString = (String) operand.get("inputTables");
        String providerClassName = (String) operand.get("provider");

        JsonArray inputTables;
        try (JsonReader reader = Json.createReader(new StringReader(inputTablesJsonArrayString))) {
            inputTables = reader.readArray();
        }
        List<LakeTable> tables = inputTables.stream()
                .map(v -> (JsonObject) v)
                .map(TableSpecification::new)
                .map(s -> new LakeTable(s, getProviderFactory(s, providerClassName)))
                .collect(Collectors.toList());


        return new LakeSchema(tables);
    }


    public LakeProviderFactory getProviderFactory(TableSpecification specification, String providerClassName) {
        if (LakeS3SelectProvider.class.getName().equals(providerClassName)) {
            return (filters, projects, fieldTypes) -> new LakeS3SelectProvider(specification.location, specification.format, filters, projects, fieldTypes);
        }
        if (LakeS3GetProvider.class.getName().equals(providerClassName)) {
            return (filters, projects, fieldTypes) -> new LakeS3GetProvider(specification.location, projects, fieldTypes);
        }
        throw new IllegalArgumentException("encountered unknown provider class name: " + providerClassName);
    }
}
