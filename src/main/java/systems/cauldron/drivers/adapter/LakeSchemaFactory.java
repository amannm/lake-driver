package systems.cauldron.drivers.adapter;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import systems.cauldron.drivers.config.TableSpecification;
import systems.cauldron.drivers.provider.LakeProviderFactory;
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

        Class<?> providerClass = extractProviderOperand(operand);

        JsonArray inputTables = extractInputTablesOperand(operand);

        List<LakeTable> tables = inputTables.stream()
                .map(v -> (JsonObject) v)
                .map(TableSpecification::new)
                .map(s -> {
                    LakeProviderFactory factory = resolveFactory(providerClass, s);
                    return new LakeTable(factory, s);
                })
                .collect(Collectors.toList());

        return new LakeSchema(tables);
    }

    private Class<?> extractProviderOperand(Map<String, Object> operand) {
        String value = (String) operand.get("provider");
        try {
            return Class.forName(value);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("encountered unknown provider class: " + value);
        }
    }

    private JsonArray extractInputTablesOperand(Map<String, Object> operand) {
        String value = (String) operand.get("inputTables");
        try (JsonReader reader = Json.createReader(new StringReader(value))) {
            return reader.readArray();
        }
    }

    private static LakeProviderFactory resolveFactory(Class<?> providerClass, TableSpecification specification) {
        if (LakeS3SelectProvider.class.equals(providerClass)) {
            return (filters, projects, fieldTypes) -> new LakeS3SelectProvider(specification.location, projects, fieldTypes, specification.format, filters);
        }
        if (LakeS3GetProvider.class.equals(providerClass)) {
            return (filters, projects, fieldTypes) -> new LakeS3GetProvider(specification.location, projects, fieldTypes);
        }
        throw new IllegalArgumentException("encountered unknown provider class: " + providerClass.getName());
    }

}
